/**
 *    Copyright (C) 2024-present Codein, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/storage/csv_file.h"

#include <charconv>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fmt/format.h>
#include <fstream>  // IWYU pragma: keep
#include <memory>
#include <string>
#include <sys/mman.h>
#include <unistd.h>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/oid.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/storage/default_path.h"
#include "mongo/db/storage/io_error_message.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {

using namespace fmt::literals;

CsvFileInput::CsvFileInput(const std::string& fileRelativePath,
                           const std::string& metadataRelativePath)
    : _fileAbsolutePath((externalFileDir == "" ? kDefaultFilePath : externalFileDir) +
                        fileRelativePath),
      _metadataAbsolutePath((externalFileDir == "" ? kDefaultFilePath : externalFileDir) +
                            metadataRelativePath),
      _ioStats(std::make_unique<CsvFileIoStats>()) {
    uassert(200000400,
            "File path must not include '..' but {} does"_format(_fileAbsolutePath),
            _fileAbsolutePath.find("..") == std::string::npos);
    uassert(200000401,
            "File path must not include '..' but {} does"_format(_metadataAbsolutePath),
            _metadataAbsolutePath.find("..") == std::string::npos);
}

CsvFileInput::~CsvFileInput() {
    StreamableInput::close();
}

void CsvFileInput::doOpen() {
    namespace fs = std::filesystem;

    std::string metadata;
    std::ifstream ifsMetadata(_metadataAbsolutePath.c_str(), std::ios::in);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}: "_format(getErrorMessage("open", _metadataAbsolutePath)),
            ifsMetadata.is_open());
    std::getline(ifsMetadata, metadata);

    _metadata = getMetadata(parseLine(metadata));
    ifsMetadata.close();

    _fd = ::open(_fileAbsolutePath.c_str(), O_RDONLY);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getErrorMessage("open", _fileAbsolutePath)),
            _fd >= 0);

    fs::path file = _fileAbsolutePath;
    _fileSize = fs::file_size(file);

    _data = (const char*)mmap(nullptr, _fileSize, PROT_READ, MAP_PRIVATE, _fd, 0);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getErrorMessage("mmap", _fileAbsolutePath)),
            _data != MAP_FAILED);
}

// Caller must ensure that buffer size is greater than or equal to the size of the bsonObject to be
// returned. If not, it will throw an exception (Not enough size in buffer).
int CsvFileInput::doRead(char* data, int size) {
    auto bsonObj = readBsonObj();

    if (!bsonObj) {
        return 0;
    }

    _ioStats->_bsonsReturned++;
    int nRead = bsonObj->objsize();
    tassert(200000402,
            "Buff Size {} bytes is too small to contain {} bytes bsonObj"_format(size, nRead),
            nRead <= size);

    _ioStats->_outputSize += nRead;
    memcpy(data, bsonObj->objdata(), nRead);
    return nRead;
}

void CsvFileInput::doClose() {
    if (_data && _data != MAP_FAILED) {
        (void)munmap((void*)_data, _fileSize);
        _data = nullptr;
    }

    if (_fd >= 0) {
        ::close(_fd);
        _fd = -1;
    }
}

bool CsvFileInput::isOpen() const {
    return _fd >= 0 && _data && _data != MAP_FAILED;
}

bool CsvFileInput::isGood() const {
    return !isFailed() && !isEof();
}

bool CsvFileInput::isFailed() const {
    return _fd >= 0 && _data == MAP_FAILED;
}

bool CsvFileInput::isEof() const {
    return _fd >= 0 && _offset >= _fileSize;
}

// Assuming that header is returned by parseLine, which means, each of its element contains name of
// the field with its typeInfo as string. {"fieldName1/typeName1","fieldName/typeName1"...}
Metadata CsvFileInput::getMetadata(const std::vector<std::string_view>& header) {
    constexpr size_t typeNameAbsent = 0;
    Metadata ret;

    size_t fieldIndex = 0;
    for (const auto& field : header) {
        // Throws an exception when field does not contain '/' at all or typename is absent after
        // '/'.
        size_t separatorIndex = field.find('/');
        size_t fieldLen = separatorIndex != std::string::npos ? field.length() - separatorIndex - 1
                                                              : typeNameAbsent;
        uassert(200000403,
                "{}th Field '{}' does not specify typeName."_format(field, fieldIndex),
                fieldLen > typeNameAbsent);

        auto fieldName = field.substr(0, separatorIndex);
        auto typeName = field.substr(separatorIndex + 1, fieldLen);
        CsvFieldType fieldType;

        if (typeName == "int" || typeName == "int32") {
            fieldType = CsvFieldType::kInt32;
        } else if (typeName == "int64" || typeName == "long") {
            fieldType = CsvFieldType::kInt64;
        } else if (typeName == "double") {
            fieldType = CsvFieldType::kDouble;
        } else if (typeName == "bool") {
            fieldType = CsvFieldType::kBool;
        } else if (typeName == "oid") {
            fieldType = CsvFieldType::kOid;
        } else if (typeName == "date") {
            fieldType = CsvFieldType::kDate;
        } else if (typeName == "string") {
            fieldType = CsvFieldType::kString;
        } else {
            uasserted(200000404,
                      "{} type is not supported at {}th field: {}"_format(
                          typeName, fieldIndex, fieldName));
        }

        ret.push_back({std::string{fieldName}, fieldType});
        fieldIndex++;
    }

    return ret;
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kInt32>(BSONObjBuilder& builder,
                                                  const std::string& fieldName,
                                                  const std::string_view& field) {
    int converted;
    auto res = std::from_chars(field.begin(), field.end(), converted);
    if (res.ec == std::errc::invalid_argument) {
        _ioStats->incInvalidInt32();
        builder.appendNull(fieldName);
        return;
    }
    if (res.ec == std::errc::result_out_of_range) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }

    if (res.ptr != field.end()) {
        _ioStats->incIncompleteConversionToNumeric();
    }
    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kDouble>(BSONObjBuilder& builder,
                                                   const std::string& fieldName,
                                                   const std::string_view& field) {
#ifdef __APPLE__
    double converted;
    try {
        converted = stod(std::string{field});
    } catch (const std::invalid_argument&) {
        _ioStats->incInvalidDouble();
        builder.appendNull(fieldName);
        return;
    } catch (const std::out_of_range&) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }
#else
    double converted;
    auto res = std::from_chars(field.begin(), field.end(), converted);
    if (res.ec == std::errc::invalid_argument) {
        _ioStats->incInvalidDouble();
        builder.appendNull(fieldName);
        return;
    }
    if (res.ec == std::errc::result_out_of_range) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }
#endif

    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kInt64>(BSONObjBuilder& builder,
                                                  const std::string& fieldName,
                                                  const std::string_view& field) {
    long long converted;
    auto res = std::from_chars(field.begin(), field.end(), converted);
    if (res.ec == std::errc::invalid_argument) {
        _ioStats->incInvalidInt64();
        builder.appendNull(fieldName);
        return;
    }
    if (res.ec == std::errc::result_out_of_range) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }

    if (res.ptr != field.end()) {
        _ioStats->incIncompleteConversionToNumeric();
    }
    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kString>(BSONObjBuilder& builder,
                                                   const std::string& fieldName,
                                                   const std::string_view& field) {
    size_t sz = field.size();
    size_t copied = 0;
    char cpyStr[sz];
    for (size_t i = 0; i < sz; i++) {
        cpyStr[copied] = field[i];
        if (field[i] == '\"') {  // Skipping the escaping double quote.
            i++;
        }
        copied++;
    }
    builder.append(fieldName, StringData{cpyStr, copied});
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kDate>(BSONObjBuilder& builder,
                                                 const std::string& fieldName,
                                                 const std::string_view& field) {
    auto date = dateFromISOString(StringData{field.begin(), field.length()});
    if (!date.isOK()) {
        _ioStats->incInvalidDate();
        builder.appendNull(fieldName);
        return;
    }
    builder.appendDate(fieldName, date.getValue());
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kOid>(BSONObjBuilder& builder,
                                                const std::string& fieldName,
                                                const std::string_view& field) {
    constexpr size_t kLengthOidValue = 24;

    // checking if the oid is enclosed by double quote or not
    std::string_view fieldData = field.front() == '\"' && field.back() == '\"'
        ? std::string_view(field.begin() + 2, field.end() - 2)
        : std::string_view(field.begin(), field.end());

    if (fieldData.length() != kLengthOidValue) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    mongo::OID id;
    auto strData = StringData(fieldData.data(), kLengthOidValue);

    try {
        id = mongo::OID(strData);  // Throws exception if it detects bad OID.
    } catch (const ExceptionFor<ErrorCodes::FailedToParse>&) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    builder.append(fieldName, id);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kBool>(BSONObjBuilder& builder,
                                                 const std::string& fieldName,
                                                 const std::string_view& field) {
    std::string mutableData{field.begin(), field.end()};
    bool val;
    std::transform(field.begin(), field.end(), mutableData.begin(), ::tolower);
    if (mutableData == "true" || mutableData == "t" || mutableData == "yes" || mutableData == "y" ||
        mutableData == "1")
        val = true;
    else if (mutableData == "false" || mutableData == "f" || mutableData == "no" ||
             mutableData == "n" || mutableData == "0")
        val = false;
    else {
        _ioStats->incInvalidBoolean();
        builder.appendNull(fieldName);
        return;
    }
    builder.append(fieldName, val);
}

std::string_view CsvFileInput::getLine() {
    dassert(_offset <= _fileSize);

    size_t start = _offset;
    bool quoteOpen = _data[_offset] == '\"';
    if (quoteOpen) {
        _offset++;
    }

    while (_offset < _fileSize && (_data[_offset] != '\n' || quoteOpen)) {
        if (_data[_offset] == '\"') {
            if (!quoteOpen && _data[_offset - 1] == ',') {  // if beginning of the field, open quote
                quoteOpen = true;
            }  // if quote is open and single quote, close the quote
            else if (quoteOpen && _data[_offset + 1] != '\"') {
                quoteOpen = false;
            }  // if quote is open and double quote, extra increment _offset
            else if (quoteOpen && _data[_offset + 1] == '\"') {
                _offset++;
            }
            // Quote that appears in the unenclosed middle of field(aaa"""bb""cc"), or  appears
            // after closing quote, but before the end of the field("aaa"bbb"), should be ignored
        }
        ++_offset;
    }
    size_t len = [&] {
        // DOS format.
        if (_offset > 1 && _data[_offset - 1] == '\r') {
            _ioStats->incDosFmt();
            return _offset - 1 - start;
        }

        // Unix format.
        auto len = _offset - start;
        _ioStats->incUnixFmt();
        return len;
    }();

    // Now makes '_offset' point to the next char to read.
    ++_offset;

    return std::string_view{_data + start, len};
}

boost::optional<BSONObj> CsvFileInput::readBsonObj() {
    if (!isGood()) {
        return boost::none;
    }

    // Ignores empty lines.
    auto line = getLine();
    while (line.empty()) {
        line = getLine();
    }

    _ioStats->_inputSize += line.size();
    auto fields = parseLine(line);

    // If data and metadata have different number of fields, process as many fields as possible.
    if (fields.size() != _metadata.size()) {
        _ioStats->incNonCompliantWithMetadata();
    }

    BSONObjBuilder builder;
    size_t processableFields = std::min(fields.size(), _metadata.size());
    for (size_t i = 0; i < processableFields; ++i) {
        if (fields[i] == "") {
            builder.appendNull(_metadata[i].fieldName);
            continue;
        }

        switch (_metadata[i].fieldType) {
            case CsvFieldType::kInt32:
                appendTo<CsvFieldType::kInt32>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kDouble:
                appendTo<CsvFieldType::kDouble>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kInt64:
                appendTo<CsvFieldType::kInt64>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kString:
                appendTo<CsvFieldType::kString>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kBool:
                appendTo<CsvFieldType::kBool>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kOid:
                appendTo<CsvFieldType::kOid>(builder, _metadata[i].fieldName, fields[i]);
                break;
            case CsvFieldType::kDate:
                appendTo<CsvFieldType::kDate>(builder, _metadata[i].fieldName, fields[i]);
                break;
        }
    }
    return builder.done().getOwned();
}

/**  State Machine Diagram:
 * Start State: ⬇︎  ︎         cur_char != "
 * +--------------+  The field is NOT enclosed    +----------------+
 * |    State:    | ----------------------------> |     State:     |-----
 * |  begindField |                               |  quote_Closed  |     | regular characters
 * |              | <---------------------------- |                | <---
 * +--------------+ <___            cur_char = ,  +----------------+ ---> cur_char = " illegal
 *   |  ⬆︎               \             End of field               double quote, RFC Non compliant,
 *   |__| cur_char = ,    \_____
 *   |    Empty field           |___________
 *   |                                      \
 *   |                                       \
 *   | cur_char = "                           ︎\ ︎ ︎  cur_char = ,
 *   | The field is enclosed by                \   Meaning next character after closing quote is
 *   | Double Quotes                            \  comma, (",) Correct syntax for end of field
 *   ⬇︎ ︎                                          \
 * +--------------+       cur_char = "            +----------------+
 * |     State:   | ----------------------------> |      State:    | other characters
 * |  quote_open  |                               | checkForDouble | ---> RFC Non compliant
 * |              | <---------------------------- |     -Quote     |
 * +--------------+       cur_char = "            +----------------+
 *    |   ⬆︎          Double double quote (""),
 *    |   |           Escaped double quote
 *    |___|
 *   Any character, except double quote "
 */

std::vector<std::string_view> CsvFileInput::parseLine(const std::string_view& line) {
    auto stateMachine = parsingState::beginField;

    const size_t len = line.length();
    size_t i = 0;
    size_t left = 0;
    std::vector<std::string_view> fields;

    while (i <= len) {
        switch (stateMachine) {
            case parsingState::beginField:
                left = i;
                if (i == len || line[i] == ',') {  // Empty field.
                    fields.emplace_back(line.substr(left, 0));
                    left++;
                    break;
                }
                stateMachine =
                    line[i] == '\"' ? parsingState::quoteOpen : parsingState::quoteClosed;
                break;
            case parsingState::quoteOpen:
                if (i == len) {  // Field that does not comply with RFC 4180
                    _ioStats->incNonCompliantWithRFC();
                    return {};
                }
                if (line[i] == '\"') {
                    stateMachine = parsingState::checkForDoubleQuote;
                }
                break;
            case parsingState::quoteClosed:
                if (i == len || line[i] == ',') {  // End of Field.
                    fields.emplace_back(line.substr(left, i - left));
                    stateMachine = parsingState::beginField;
                } else if (line[i] == '\"') {  // Field that does not comply with RFC 4180
                    _ioStats->incNonCompliantWithRFC();
                    return {};
                }
                break;
            case parsingState::checkForDoubleQuote:
                if (i == len || line[i] == ',') {  // End of field.
                    fields.emplace_back(line.substr(left + 1, i - 2 - left));
                    stateMachine = parsingState::beginField;
                } else if (line[i] == '\"') {  // Escaping double quote.
                    stateMachine = parsingState::quoteOpen;
                } else {  // Field that does not comply with RFC 4180
                    _ioStats->incNonCompliantWithRFC();
                    return {};
                }
                break;
        }
        i++;
    }

    return fields;
}


}  // namespace mongo
