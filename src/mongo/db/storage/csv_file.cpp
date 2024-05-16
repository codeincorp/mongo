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

#include <fmt/format.h>
#include <fstream>  // IWYU pragma: keep
#include <memory>
#include <string>

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
      _ifs(),
      _ioStats(std::make_unique<CsvFileIoStats>()) {
    uassert(200000400,
            "File path must not include '..' but {} does"_format(_fileAbsolutePath),
            _fileAbsolutePath.find("..") == std::string::npos);
    uassert(200000401,
            "File path must not include '..' but {} does"_format(_metadataAbsolutePath),
            _metadataAbsolutePath.find("..") == std::string::npos);
}

CsvFileInput::~CsvFileInput() {
    _ifs.close();
}

void CsvFileInput::doOpen() {
    std::string metadata;
    std::ifstream ifsMetadata(_metadataAbsolutePath.c_str(), std::ios::in);
    uassert(ErrorCodes::FileNotOpen,
            "error = {}: "_format(getErrorMessage("open", _metadataAbsolutePath)),
            ifsMetadata.is_open());
    std::getline(ifsMetadata, metadata);

    _metadata = getMetadata(parseLine(metadata));
    ifsMetadata.close();

    _ifs.open(_fileAbsolutePath.c_str(), std::ios::in);

    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getErrorMessage("open", _fileAbsolutePath)),
            _ifs.is_open());
}

// Caller must ensure that buffer size is greater than or equal to the size of the bsonObject to be
// returned. If not, it will throw an exception (Not enough size in buffer).
int CsvFileInput::doRead(char* data, int size) {
    auto bsonObj = readBsonObj();

    if (!bsonObj) {
        return 0;
    }

    int nRead = bsonObj->objsize();
    tassert(200000402,
            "Buff Size {} bytes is too small to contain {} bytes bsonObj"_format(size, nRead),
            nRead <= size);

    _ioStats->_outputSize += nRead;
    memcpy(data, bsonObj->objdata(), nRead);
    return nRead;
}

void CsvFileInput::doClose() {
    _ifs.close();
}

bool CsvFileInput::isOpen() const {
    return _ifs.is_open();
}

bool CsvFileInput::isGood() const {
    return _ifs.good();
}

bool CsvFileInput::isFailed() const {
    return _ifs.fail();
}

bool CsvFileInput::isEof() const {
    return _ifs.eof();
}

// Assuming that header is returned by parseLine, which means, each of its element contains name of
// the field with its typeInfo as string. {"fieldName1/typeName1","fieldName/typeName1"...}
Metadata CsvFileInput::getMetadata(const std::vector<std::string>& header) {
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

        ret.push_back({std::move(fieldName), fieldType});
        fieldIndex++;
    }

    return ret;
}


template <>
void CsvFileInput::appendTo<CsvFieldType::kInt32>(BSONObjBuilder& builder,
                                                  const std::string& fieldName,
                                                  const std::string& data) {
    int converted;
    size_t idx;
    try {
        converted = stoi(data, &idx);
    } catch (const std::invalid_argument&) {
        _ioStats->incInvalidInt32();
        builder.appendNull(fieldName);
        return;
    } catch (const std::out_of_range&) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }

    if (idx != data.length()) {
        _ioStats->incIncompleteConversionToNumeric();
    }
    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kDouble>(BSONObjBuilder& builder,
                                                   const std::string& fieldName,
                                                   const std::string& data) {
    double converted;
    try {
        converted = stod(data);
    } catch (const std::invalid_argument&) {
        _ioStats->incInvalidDouble();
        builder.appendNull(fieldName);
        return;
    } catch (const std::out_of_range&) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }

    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kInt64>(BSONObjBuilder& builder,
                                                  const std::string& fieldName,
                                                  const std::string& data) {
    long long converted;
    size_t idx;
    try {
        converted = stoll(data, &idx);
    } catch (const std::invalid_argument&) {
        _ioStats->incInvalidInt64();
        builder.appendNull(fieldName);
        return;
    } catch (const std::out_of_range&) {
        _ioStats->incOutOfRange();
        builder.appendNull(fieldName);
        return;
    }

    if (idx != data.length()) {
        _ioStats->incIncompleteConversionToNumeric();
    }
    builder.append(fieldName, converted);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kString>(BSONObjBuilder& builder,
                                                   const std::string& fieldName,
                                                   const std::string& data) {
    builder.append(fieldName, data);
}

template <>
void CsvFileInput::appendTo<CsvFieldType::kDate>(BSONObjBuilder& builder,
                                                 const std::string& fieldName,
                                                 const std::string& data) {
    auto date = dateFromISOString(data);
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
                                                const std::string& data) {
    constexpr size_t kLengthOidValue = 24;
    constexpr size_t kOidTypeStr = 8;                      // Length of 'objectid'.
    constexpr size_t kOidTypeStrPrefix = kOidTypeStr + 2;  // Length of 'objectid(\"'.

    std::string mutableData = data;
    if (data[0] == 'O' || data[0] == 'o') {
        std::transform(
            mutableData.begin(), mutableData.begin() + kOidTypeStr, mutableData.begin(), ::tolower);

        if (mutableData.substr(0, kOidTypeStrPrefix) != "objectid(\"") {
            _ioStats->incInvalidOid();
            builder.appendNull(fieldName);
            return;
        }

        mutableData = mutableData.substr(kOidTypeStrPrefix, kLengthOidValue);
    } else if (data[0] == '\"') {
        mutableData = mutableData.substr(1, kLengthOidValue);
    }

    if (mutableData.length() != kLengthOidValue) {
        _ioStats->incInvalidOid();
        builder.appendNull(fieldName);
        return;
    }

    mongo::OID id;
    try {
        id = mongo::OID(mutableData);  // Throws exception if it detects bad OID.
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
                                                 const std::string& data) {
    std::string mutableData = data;
    bool val;
    std::transform(data.begin(), data.end(), mutableData.begin(), ::tolower);
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

boost::optional<BSONObj> CsvFileInput::readBsonObj() {
    std::string record;
    std::getline(_ifs, record);
    // If eof is reached, _ifs return false.
    if (_ifs.eof() || _ifs.fail()) {
        return boost::none;
    }

    _ioStats->_inputSize += record.size();
    _ioStats->_bsonsReturned++;
    auto data = parseLine(record);

    // If data and metadata have different number of fields, process as many fields as possible.
    if (data.size() != _metadata.size()) {
        _ioStats->incNonCompliantWithMetadata();
    }

    BSONObjBuilder builder;
    size_t processableFields = std::min(data.size(), _metadata.size());
    for (size_t i = 0; i < processableFields; ++i) {
        if (data[i] == "") {
            builder.appendNull(_metadata[i].fieldName);
            continue;
        }

        switch (_metadata[i].fieldType) {
            case CsvFieldType::kInt32:
                appendTo<CsvFieldType::kInt32>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kDouble:
                appendTo<CsvFieldType::kDouble>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kInt64:
                appendTo<CsvFieldType::kInt64>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kString:
                appendTo<CsvFieldType::kString>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kBool:
                appendTo<CsvFieldType::kBool>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kOid:
                appendTo<CsvFieldType::kOid>(builder, _metadata[i].fieldName, data[i]);
                break;
            case CsvFieldType::kDate:
                appendTo<CsvFieldType::kDate>(builder, _metadata[i].fieldName, data[i]);
                break;
        }
    }
    return builder.done().getOwned();
}

std::vector<std::string> CsvFileInput::parseLine(const std::string& line) {
    int left = 0;
    int right = 0;
    int len = line.length();
    int i = 0;
    std::vector<std::string> strs;

    while (i <= len) {
        if (i == len || line.at(i) == ',') {
            strs.emplace_back(line.substr(left, right - left));
            left = 0;
            right = 0;
        } else if (line.at(i) != ' ' && line.at(i) != '\t') {
            if (right == 0) {
                left = i;
            }
            right = i + 1;
        }

        i++;
    }
    return strs;
}

}  // namespace mongo
