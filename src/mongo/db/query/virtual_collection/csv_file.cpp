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

#include "mongo/db/query/virtual_collection/csv_file.h"

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
#include "mongo/logv2/log.h"
#include "mongo/transport/named_pipe/io_error_message.h"
#include "mongo/util/assert_util.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kCommand

namespace mongo {

using namespace fmt::literals;

CsvFileInput::CsvFileInput(const std::string& fileRelativePath, BSONObj metadataObj)
    : _fileAbsolutePath((externalFileDir == "" ? kDefaultFilePath : externalFileDir) +
                        fileRelativePath),
      _metadata(getMetadata(metadataObj.getOwned())),
      _ioStats(std::make_unique<CsvFileIoStats>()) {
    uassert(200000400,
            "File path must not include '..' but {} does"_format(_fileAbsolutePath),
            _fileAbsolutePath.find("..") == std::string::npos);
}

CsvFileInput::CsvFileInput(const std::string& fileRelativePath,
                           const std::string& metadataRelativePath)
    : _fileAbsolutePath((externalFileDir == "" ? kDefaultFilePath : externalFileDir) +
                        fileRelativePath),
      _metadata(getMetadata(metadataRelativePath)),
      _ioStats(std::make_unique<CsvFileIoStats>()) {
    uassert(200003500,
            "File path must not include '..' but {} does"_format(_fileAbsolutePath),
            _fileAbsolutePath.find("..") == std::string::npos);
}

CsvFileInput::~CsvFileInput() {
    StreamableInput::close();
}

void CsvFileInput::doOpen() {
    namespace fs = std::filesystem;
    _fd = ::open(_fileAbsolutePath.c_str(), O_RDONLY);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getLastSystemErrorMessageFormatted("open", _fileAbsolutePath)),
            _fd >= 0);

    fs::path file = _fileAbsolutePath;
    _fileSize = fs::file_size(file);

    _data = (const char*)mmap(nullptr, _fileSize, PROT_READ, MAP_PRIVATE, _fd, 0);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getLastSystemErrorMessageFormatted("mmap", _fileAbsolutePath)),
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

template <typename T, typename U>
CsvFieldType fromTypeName(T fieldName, U typeName) {
    if (typeName == "int" || typeName == "int32") {
        return CsvFieldType::kInt32;
    } else if (typeName == "int64" || typeName == "long") {
        return CsvFieldType::kInt64;
    } else if (typeName == "double") {
        return CsvFieldType::kDouble;
    } else if (typeName == "bool") {
        return CsvFieldType::kBool;
    } else if (typeName == "oid") {
        return CsvFieldType::kOid;
    } else if (typeName == "date") {
        return CsvFieldType::kDate;
    } else if (typeName == "string") {
        return CsvFieldType::kString;
    } else {
        uasserted(200000404, "{} type is not supported at field: {}"_format(typeName, fieldName));
    }
}

// Assuming that header is returned by parseLine, which means, each of its element contains name of
// the field with its typeInfo as string. {"fieldName1/typeName1","fieldName/typeName1"...}
Metadata CsvFileInput::getMetadata(const std::string& metadataRelativePath) {
    auto metadataAbsolutePath((externalFileDir == "" ? kDefaultFilePath : externalFileDir) +
                              metadataRelativePath);
    uassert(200000401,
            "File path must not include '..' but {} does"_format(metadataAbsolutePath),
            metadataAbsolutePath.find("..") == std::string::npos);

    std::ifstream ifsMetadata(metadataAbsolutePath.c_str(), std::ios::in);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}: "_format(getLastSystemErrorMessageFormatted("open", metadataAbsolutePath)),
            ifsMetadata.is_open());
    std::string metadata;
    std::getline(ifsMetadata, metadata);
    ifsMetadata.close();

    constexpr size_t typeNameAbsent = 0;
    Metadata ret;

    size_t fieldIndex = 0;
    for (const auto& field : parseRecord(metadata)) {
        // Throws an exception when field does not contain '/' at all or typename is absent after
        // '/'.
        size_t separatorIndex = field.find('/');
        size_t fieldLen = separatorIndex != std::string::npos ? field.length() - separatorIndex - 1
                                                              : typeNameAbsent;
        uassert(200000403,
                "{}th Field '{}' does not specify typeName."_format(fieldIndex, field),
                fieldLen > typeNameAbsent);

        auto fieldName = field.substr(0, separatorIndex);
        auto typeName = field.substr(separatorIndex + 1, fieldLen);
        auto fieldType = fromTypeName(fieldName, typeName);

        ret.push_back({std::string{fieldName}, fieldType});
        fieldIndex++;
    }

    return ret;
}

Metadata CsvFileInput::getMetadata(BSONObj metadataObj) {
    Metadata ret;

    for (auto&& elem : metadataObj) {
        auto fieldName = elem.fieldName();
        uassert(200003501,
                "Expected a string for {} but got {}"_format(fieldName, typeName(elem.type())),
                elem.type() == BSONType::String);
        auto typeName = elem.valueStringData();
        auto fieldType = fromTypeName(fieldName, typeName);

        ret.push_back({std::string{fieldName}, fieldType});
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
    constexpr size_t kCopyStrBufSize = 65536;
    const size_t sz = field.size();
    uassert(
        200000900, "The string is too big at offset = {}"_format(_offset), sz <= kCopyStrBufSize);
    size_t nCopied = 0;

    size_t start = 0;
    for (size_t i = 0; i < sz; i++) {
        if (field[i] == '\"') {
            // Skipping the escaping double quote.
            i++;

            if (!_copyStr) {
                _copyStr.reset(new char[kCopyStrBufSize]);
            }
            memcpy(_copyStr.get() + nCopied, field.data() + start, i - start);
            nCopied += (i - start);
            start = i + 1;
        }
    }

    if (nCopied == 0) {
        // If there was no double quote in the string_view, directly append the field
        builder.append(fieldName, StringData{field.data(), sz});
    } else {
        // else, append the last substring
        memcpy(_copyStr.get() + nCopied, field.data() + start, sz - start);
        nCopied += (sz - start);
        builder.append(fieldName, StringData{_copyStr.get(), nCopied});
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

    bool hasPrefix = boost::iequals(field.substr(0, oidPrefixLen), "objectid(\"\"") &&
        field[field.size() - 1] == ')';

    // Check if the oid is formatted as objectId("1234....") and if it is, slice off the prefix
    // 'objectId("', which is 11 characters and suffix '")'. Otherwise, Check if the oid is enclosed
    // by double quote or not. If so, since it should be RFC compliant, it would be surrounded by
    // double double-quotes, like ""1234...""
    std::string_view fieldData = [&] {
        if (hasPrefix) {
            return std::string_view(field.begin() + oidPrefixLen, field.end() - oidSufixLen);
        }
        return field.front() == '\"' && field.back() == '\"'
            ? std::string_view(field.begin() + quotedOid, field.end() - quotedOid)
            : std::string_view(field.begin(), field.end());
    }();

    if (fieldData.length() != kLengthOidValue) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    auto swOid = OID::parse(StringData(fieldData.data(), kLengthOidValue));
    if (!swOid.isOK()) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    builder.append(fieldName, swOid.getValue());
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
    // If the first field is quoted, consumes the first character.
    bool quoteOpen = _data[_offset] == '\"';
    if (quoteOpen) {
        _offset++;
    }

    bool badDoubleQuote = false;
    while (_offset < _fileSize && (_data[_offset] != '\n' || quoteOpen)) {
        if (_data[_offset] == '\"') {
            // We should handle the double quote specially according to RFC4180.
            if (!quoteOpen && _data[_offset - 1] == ',') {
                // Opens the quote only if it's the beginning of the field.
                quoteOpen = true;
            } else if (quoteOpen &&
                       (_offset + 1 >= _fileSize || _data[_offset + 1] == ',' ||
                        _data[_offset + 1] == '\r' || _data[_offset + 1] == '\n')) {
                // If quote is open and we reached end of field, close the quote. End of field is
                // reached when:
                // - The '_offset' reached the end of the file or
                // - The next character is:
                //   - comma: end of field in the middle of a record,
                //   - DOS format carriage return (\r), or Unix format new line(\n) : end of the
                //     last field.
                quoteOpen = false;
            } else if (quoteOpen && (_offset + 1 < _fileSize && _data[_offset + 1] == '\"')) {
                // if quote is open and we found an escaped double quote (""), increment _offset one
                // more
                _offset++;
            } else {
                badDoubleQuote = true;
                break;
            }
        }
        ++_offset;
    }

    if (badDoubleQuote || quoteOpen) {
        using namespace std;  // for ""_sv literal operator.
        // When detecting a bad double quote that violates RFC4180 or reached the end of the file
        // without closing the previous double quote, immediately stop reading the csv file.
        // E.g: aaa"""bb""cc",field("aaa"bbb"),"aaa\n.
        LOGV2_WARNING(200000901,
                      "File content is not compliant with the RFC4180. The rest of file is ignored",
                      "filePath"_attr = _fileAbsolutePath,
                      "offset"_attr = _offset);
        _offset = _fileSize;
        // Returns empty record.
        return ""sv;
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
 * Since getRecord() does not allow RFC-violating record, parseRecord() assumes that bad double
 * quote does not exist, therefore does not have any check against it
 *
 * Start State: ⬇︎  ︎
 * +--------------+
 * |    State:    |
 * |  notQuoted   |⟲ cur_char = , or End of field
 * |              |
 * +--------------+ <-------------------+
 *   |                                   \
 *   |                                    \
 *   |                                     \
 *   |                                      \
 *   |                                       \ Any character except double quote:
 *   | cur_char = "                           ︎\ ︎ ︎  Otherwise, it can only be comma, since
 *   | The field is enclosed by                \   getRecord() does not allow RFC-violating record.
 *   | double quotes                            \  Thus, if cur_char is not ", it must be the end of
 *   ⬇︎ ︎                                          \ the field.
 * +--------------+        cur_char = "           +----------------+
 * |    State:    | ----------------------------> |      State:    | other characters
 * |    quoted    |                               | checkForEscaped| ---> RFC Non compliant
 * |              | <---------------------------- |  DoubleQuote   |
 * +--------------+       cur_char = "            +----------------+
 *    |   ⬆︎          escaped double-quote (""),
 *    |   |
 *    +---+
 *   Any character except double quote "
 */

std::vector<std::string_view> CsvFileInput::parseRecord(const std::string_view& record) {
    auto state = ParsingState::notQuoted;

    const size_t len = record.length();
    size_t curPos = 0;
    size_t fieldStart = 0;
    std::vector<std::string_view> fields;

    while (curPos <= len) {
        switch (state) {
            case ParsingState::notQuoted:
                if (curPos == len || record[curPos] == ',') {
                    // End of Field.
                    fields.emplace_back(record.substr(fieldStart, curPos - fieldStart));
                    fieldStart = curPos + 1;
                } else if (record[curPos] == '\"') {
                    // Beginning of quoted field.
                    state = ParsingState::quoted;
                }
                break;
            case ParsingState::quoted:
                if (record[curPos] == '\"') {
                    state = ParsingState::checkForEscapedDoubleQuote;
                }
                break;
            case ParsingState::checkForEscapedDoubleQuote:
                if (record[curPos] == '\"') {
                    // Escaping double quote.
                    state = ParsingState::quoted;
                } else {
                    // End of field, discard the surrounding double quotes.
                    fields.emplace_back(record.substr(fieldStart + 1, curPos - 2 - fieldStart));
                    state = ParsingState::notQuoted;
                    fieldStart = curPos + 1;
                }
                break;
        }
        curPos++;
    }

    return fields;
}

}  // namespace mongo
