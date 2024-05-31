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

#include <boost/algorithm/string.hpp>
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

    _metadata = getMetadata(parseRecord(metadata));
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
    const size_t sz = field.size();
    uassert(200000900, "The string too big at offset = {}"_format(_offset), sz <= 65536);
    size_t nCopied = 0;
    char cpyStr[sz];

    size_t start = 0;

    for (size_t i = 0; i < sz; i++) {
        if (field[i] == '\"') {  // Skipping the escaping double quote.
            i++;

            memcpy(cpyStr + nCopied, field.data() + start, i - start);
            nCopied += (i - start);
            start = i + 1;
        }
    }

    if (start == 0) {  // If there was no double quote in the string_view, directly append the field
        builder.append(fieldName, StringData{field.data(), sz});
    } else {  // else, append the last substring
        memcpy(cpyStr + nCopied, field.data() + start, sz - start);
        nCopied += (sz - start);
        builder.append(fieldName, StringData{cpyStr, nCopied});
    }
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
    constexpr size_t oidPrefixLen = 11;
    constexpr size_t oidSufixLen = 3;
    constexpr size_t quotedOid = 2;

    bool objId = boost::iequals(field.substr(0, oidPrefixLen), "objectId(\"\"") &&
        field[field.size() - 1] == ')';

    // Check if the oid is formatted as objectId("1234....") and if it is, slice off the prefix
    // 'objectId("', which is 11 characters and suffic '")'. Else, Check if the oid is enclosed by
    // double quote or not. If so, since it should be RFC compliant, it would be surrounded by
    // double double-quotes, like
    // ""1234...""

    std::string_view fieldData;
    if (objId) {
        fieldData = std::string_view(field.begin() + oidPrefixLen, field.end() - oidSufixLen);
    } else {
        fieldData = field.front() == '\"' && field.back() == '\"'
            ? std::string_view(field.begin() + quotedOid, field.end() - quotedOid)
            : std::string_view(field.begin(), field.end());
    }

    if (fieldData.length() != kLengthOidValue) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    auto strData = StringData(fieldData.data(), kLengthOidValue);
    auto oid = OID::parse(strData);
    if (!oid.isOK()) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    builder.append(fieldName, oid.getValue());
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kBool>(BSONObjBuilder& builder,
                                                 const std::string& fieldName,
                                                 const std::string_view& field) {
    std::string mutableData{field.begin(), field.end()};
    bool val;

    if (boost::iequals(field, "true") || boost::iequals(field, "t") ||
        boost::iequals(field, "yes") || boost::iequals(field, "y") || boost::iequals(field, "1"))
        val = true;
    else if (boost::iequals(field, "false") || boost::iequals(field, "f") ||
             boost::iequals(field, "no") || boost::iequals(field, "n") ||
             boost::iequals(field, "0"))
        val = false;
    else {
        _ioStats->incInvalidBoolean();
        builder.appendNull(fieldName);
        return;
    }
    builder.append(fieldName, val);
}

std::string_view CsvFileInput::getRecord() {
    dassert(_offset <= _fileSize);

    size_t start = _offset;
    // If the first field is quoted, consume the first character.
    bool quoteOpen = _data[_offset] == '\"';
    if (quoteOpen) {
        _offset++;
    }

    while (_offset < _fileSize && (_data[_offset] != '\n' || quoteOpen)) {
        if (_data[_offset] == '\"') {

            if (!quoteOpen && _data[_offset - 1] == ',') {  // if beginning of the field, open quote
                quoteOpen = true;
            }
            // If quote is open and end of filed, close the quote. End of field is when: The
            // Cursor reached the end of the file, or the next character is: comma, dos format new
            // line (\r), or Unix format new line(\n).
            else if (quoteOpen &&
                     (_offset + 1 >= _fileSize || _data[_offset + 1] == ',' ||
                      _data[_offset + 1] == '\r' || _data[_offset + 1] == '\n')) {
                quoteOpen = false;
            }  // if quote is open and double quote, extra increment _offset
            else if (quoteOpen && (_offset + 1 < _fileSize && _data[_offset + 1] == '\"')) {
                _offset++;
            } else {
                LOGV2_WARNING(200000901,
                              "csvFile violates the RFC 4180 standard. The rest of the contents of "
                              "csvFile is skipped Cursor = _offset",
                              "csvFile"_attr = _fileAbsolutePath,
                              "_offset"_attr = _offset);
                _offset = _fileSize;
                return "";  // return empty record
            }
            // When CsvFileInput detects a bad quote that violates RFC 4180, it will immediately
            // stop reading the csv file. E.g: (aaa"""bb""cc"), field("aaa"bbb").
        }
        ++_offset;
    }

    if (quoteOpen) {  // Reached the end of the fiile without closing the previous double quote.
                      // Although this case will be extremely rare, it is possible.
        LOGV2_WARNING(200000901,
                      "csvFile violates the RFC 4180 standard. The rest of the contents of "
                      "csvFile is skipped Cursor = _offset",
                      "csvFile"_attr = _fileAbsolutePath,
                      "_offset"_attr = _offset);
        _offset = _fileSize;
        return "";  // return empty record
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
    auto line = getRecord();
    while (_offset < _fileSize && line.empty()) {
        line = getRecord();
    }

    _ioStats->_inputSize += line.size();
    auto fields = parseRecord(line);

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

/**
 * State Machine Diagram:
 * Since getRecord does not allow RFC-violating record, parseRecord assumes that bad double quote
 * does not exist, therefore does not have any check against it
 *
 * Start State: ⬇︎  ︎
 * +--------------+
 * |    State:    |
 * |  notQuoted   |⟲ cur_char = , End of field
 * |              |
 * +--------------+ <___
 *   |                  \
 *   |                    \_____
 *   |                          |___________
 *   |                                      \
 *   |                                       \
 *   | cur_char = "                           ︎\ ︎ ︎ Else, it can only be comma, since getRecord
 *   | The field is enclosed by                \  does not allow RFC-violating record. Thus, it
 *   | Double quotes                            \ cur_char is not ", it must be the end of the field
 *   ⬇︎ ︎                                          \
 * +--------------+        cur_char = "           +----------------+
 * |    State:    | ----------------------------> |      State:    | other characters
 * |    quoted    |                               | checkForDouble | ---> RFC Non compliant
 * |              | <---------------------------- |  Double-Quote  |
 * +--------------+       cur_char = "            +----------------+
 *    |   ⬆︎          Double double-quote (""),
 *    |   |          AKA, Escaped double quote
 *    |___|
 *   Any character, except double quote "
 */

std::vector<std::string_view> CsvFileInput::parseRecord(const std::string_view& record) {
    auto state = ParsingState::notQuoted;

    const size_t len = record.length();
    size_t i = 0;
    size_t left = 0;
    std::vector<std::string_view> fieldStart;

    while (i <= len) {
        switch (state) {
            case ParsingState::notQuoted:
                if (i == len || record[i] == ',') {  // End of Field.
                    fieldStart.emplace_back(record.substr(left, i - left));
                    left = i + 1;
                } else if (record[i] == '\"') {  // Beginning of quoted field.
                    state = ParsingState::quoted;
                }
                break;
            case ParsingState::quoted:
                if (record[i] == '\"') {
                    state = ParsingState::checkForDoubleDoubleQuote;
                }
                break;
            case ParsingState::checkForDoubleDoubleQuote:
                if (record[i] == '\"') {  // Escaping double quote.
                    state = ParsingState::quoted;
                } else {  // End of field, discard the surrounding double quotes
                    fieldStart.emplace_back(record.substr(left + 1, i - 2 - left));
                    state = ParsingState::notQuoted;
                    left = i + 1;
                }
                break;
        }
        i++;
    }

    return fieldStart;
}

}  // namespace mongo
