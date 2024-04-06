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
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <fmt/format.h>
#include <fstream>  // IWYU pragma: keep
#include <string>
#include <sys/stat.h>

#include "mongo/base/error_codes.h"
#include "mongo/base/string_data.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/storage/csv_file.h"
#include "mongo/db/storage/io_error_message.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/time_support.h"

namespace mongo {
using namespace fmt::literals;

CsvFileInput::CsvFileInput(const std::string& fileRelativePath, const std::string& metaFileRelative)
    : _fileAbsolutePath((externalPipeDir == "" ? kDefaultPipePath : externalPipeDir) +
                        fileRelativePath),
      _metadataAbsolutePath((externalPipeDir == "" ? kDefaultPipePath : externalPipeDir) +
                            metaFileRelative),
      _ifs() {
    uassert(200000400,
            "File path must not include '..' but {} does"_format(_fileAbsolutePath),
            _fileAbsolutePath.find("..") == std::string::npos);
}

CsvFileInput::~CsvFileInput() {
    _ifs.close();
}

void CsvFileInput::doOpen() {
    std::string metadata;
    _ifs.open(_metadataAbsolutePath.c_str(), std::ios::binary | std::ios::in);
    uassert(ErrorCodes::FileNotOpen,
            "File has failed to open. Make sure the file exists: "_format(
                getErrorMessage("open", _metadataAbsolutePath)),
            _ifs.is_open());
    std::getline(_ifs, metadata);

    _md = getMetadata(parseLine(metadata));
    _ifs.close();

    _ifs.open(_fileAbsolutePath.c_str(), std::ios::binary | std::ios::in);

    uassert(ErrorCodes::FileNotOpen,
            "error = {}"_format(getErrorMessage("open", _fileAbsolutePath)),
            _ifs.is_open());
}

// caller must ensure that buffer size is greater than or equal to the size of the bsonObject
// to be returned. If not, it would throw exception (Not enough size in buffer)
int CsvFileInput::doRead(char* data, int size) {

    auto bsonObj = readBsonObj();

    if (!bsonObj) {
        return 0;
    }

    int nRead = bsonObj->objsize();
    uassert(200000402, "Buff Size too small", nRead <= size);
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

// assuming that header is returned by parseLine, which means, each of its element is
// name of the field with type name
Metadata CsvFileInput::getMetadata(const std::vector<std::string>& header) {
    Metadata ret;

    for (auto& field : header) {
        int index = field.find('/');
        int len = field.length() - index - 1;

        auto fieldName = field.substr(0, index);
        auto typeName = field.substr(index + 1, len);
        CsvFieldType type;

        if (typeName == "int" || typeName == "int32") {
            type = CsvFieldType::kInt32;
        } else if (typeName == "int64" || typeName == "long") {
            type = CsvFieldType::kInt64;
        } else if (typeName == "double") {
            type = CsvFieldType::kDecimal;
        } else if (typeName == "bool") {
            type = CsvFieldType::kBool;
        } else if (typeName == "oid") {
            type = CsvFieldType::kOid;
        } else if (typeName == "date") {
            type = CsvFieldType::kDate;
        } else if (typeName == "string") {
            type = CsvFieldType::kString;
        } else {
            uasserted(200000401, typeName + " type specified in metadata is not supported");
        }

        ret.push_back({std::move(fieldName), type});
    }

    return ret;
}

template <CsvFieldType T>
void appendTo(BSONObjBuilder& builder, const std::string& fieldName, const std::string& data);

template <>
void appendTo<CsvFieldType::kInt32>(BSONObjBuilder& builder,
                                    const std::string& fieldName,
                                    const std::string& data) {
    builder.append(fieldName, stoi(data));
    if (data.find('.') != std::string::npos) {
        std::cout << std::endl
                  << "Lossy conversion of decimal type to whole number type:   Line Number: Field: "
                  << fieldName << std::endl;
    }
}

template <>
void appendTo<CsvFieldType::kDecimal>(BSONObjBuilder& builder,
                                      const std::string& fieldName,
                                      const std::string& data) {
    builder.append(fieldName, stod(data));
    int dotIndex = data.find('.');
    // Warning user of possibly unwanted lossy conversion if provided decimal is too
    // long
    if (data.substr(dotIndex + 1, data.length() - dotIndex - 1).length() > 15) {
        std::cout << std::endl
                  << "Possible lossy conversion: " << data << " Line Number:  Field: " << std::endl;
    }
}

template <>
void appendTo<CsvFieldType::kInt64>(BSONObjBuilder& builder,
                                    const std::string& fieldName,
                                    const std::string& data) {
    builder.append(fieldName, stoll(data));
    if (data.find('.') != std::string::npos) {
        std::cout << std::endl
                  << "Lossy conversion of decimal type to whole number type:   Line Number: Field: "
                  << fieldName << std::endl;
    }
}

template <>
void appendTo<CsvFieldType::kString>(BSONObjBuilder& builder,
                                     const std::string& fieldName,
                                     const std::string& data) {
    builder.append(fieldName, data);
}

template <>
void appendTo<CsvFieldType::kDate>(BSONObjBuilder& builder,
                                   const std::string& fieldName,
                                   const std::string& data) {
    auto date = dateFromISOString(data);
    // return badValue error codes
    uassert(date.getStatus().code(), date.getStatus().toString(), date.isOK());
    builder.appendDate(fieldName, date.getValue());
}

template <>
void appendTo<CsvFieldType::kOid>(BSONObjBuilder& builder,
                                  const std::string& fieldName,
                                  const std::string& data) {
    constexpr size_t kLengthOidValue = 24;
    constexpr size_t kOidTypeStr = 8;                      // objectid
    constexpr size_t kOidTypeStrPrefix = kOidTypeStr + 2;  // objectid("

    std::string mutableData = data;
    if (data[0] == 'O' || data[0] == 'o') {
        std::transform(
            mutableData.begin(), mutableData.begin() + kOidTypeStr, mutableData.begin(), ::tolower);
        uassert(ErrorCodes::BadValue,
                "Invalid Object Id format",
                mutableData.substr(0, kOidTypeStrPrefix) == "objectid(\"");
        mutableData = mutableData.substr(kOidTypeStrPrefix, kLengthOidValue);
    } else if (data[0] == '\"') {
        mutableData = mutableData.substr(1, kLengthOidValue);
    }

    uassert(
        ErrorCodes::BadValue, "Invalid Object Id Format", mutableData.length() == kLengthOidValue);
    mongo::OID id = mongo::OID(mutableData);  // throws exception if bad
    builder.append(fieldName, id);
}

template <>
void appendTo<CsvFieldType::kBool>(BSONObjBuilder& builder,
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
    else
        uasserted(ErrorCodes::BadValue, "Invalid Boolean representation");
    builder.append(fieldName, val);
}


boost::optional<BSONObj> CsvFileInput::readBsonObj() {
    std::string record;
    std::getline(_ifs, record);
    // if eof is reache, _ifs return false
    if (_ifs.eof() || _ifs.fail()) {
        return boost::none;
    }

    auto data = parseLine(record);
    uassert(ErrorCodes::TypeMismatch,
            "Metadata and current record mismatch",
            data.size() == _md.size());

    BSONObjBuilder builder;

    for (size_t i = 0; i < _md.size(); ++i) {
        if (data[i] == "") {
            builder.appendNull(_md[i].fieldName);
            continue;
        }

        try {
            switch (_md[i].fieldType) {
                case CsvFieldType::kInt32:
                    appendTo<CsvFieldType::kInt32>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kDecimal:
                    appendTo<CsvFieldType::kDecimal>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kInt64:
                    appendTo<CsvFieldType::kInt64>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kString:
                    appendTo<CsvFieldType::kString>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kBool:
                    appendTo<CsvFieldType::kBool>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kOid:
                    appendTo<CsvFieldType::kOid>(builder, _md[i].fieldName, data[i]);
                    break;
                case CsvFieldType::kDate:
                    appendTo<CsvFieldType::kDate>(builder, _md[i].fieldName, data[i]);
                    break;
            }
            // catching failed conversion to stoi, stof, stoll, and stod
        } catch (const std::invalid_argument& e) {
            uasserted(ErrorCodes::BadValue, e.what());
        } catch (const std::out_of_range& e) {
            uasserted(ErrorCodes::Overflow, e.what());
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
