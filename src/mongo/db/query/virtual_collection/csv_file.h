/**
 *    Copyright (C) 2024-present MongoDB, Inc.
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

#pragma once

#include <boost/optional.hpp>
#include <fstream>
#include <string>
#include <vector>

#include "mongo/db/storage/error_count.h"
#include "mongo/transport/named_pipe/input_object.h"

namespace mongo {

enum class CsvFieldType { kBool, kInt32, kInt64, kDate, kOid, kDouble, kString };

struct FieldInfo {
    std::string fieldName;
    CsvFieldType fieldType;
};

using Metadata = std::vector<FieldInfo>;

class CsvFileInput : public StreamableInput {
public:
    CsvFileInput(std::shared_ptr<InputStreamStats> stats,
                 const std::string& fileRelativePath,
                 const std::string& metadataRelativePath);

    ~CsvFileInput() override;
    const std::string& getAbsolutePath() const override {
        return _fileAbsolutePath;
    }

    // Factory function to create an initial ErrorCount with 0 errors.
    static std::shared_ptr<ErrorCount> createStats();

    bool isOpen() const override;
    bool isGood() const override;
    bool isFailed() const override;
    bool isEof() const override;

protected:
    void doOpen() override;
    // Read the data into pre-allocated buffer 'data' upto 'size' bytes at maximum and returns the
    // number of bytes that has been actually read.
    int doRead(char* data, int size) override;
    void doClose() override;

private:
    /**
     * Gets metadata from the header read by parseLine. Metadata contains information on the name of
     * the field and type of the field.
     *
     * @param header read from the metadata file in vector, in format {"fieldName/typeName",
     *     "fieldName/typeName"....}.
     * @return: vector of FieldInfo containing fieldName(as std::string) and typeInfo(as
     *     CsvFieldType) of the said field. {{"fieldName1",type1},{"fieldName2",type2}...}.
     */
    Metadata getMetadata(const std::vector<std::string>& header);

    /**
     * Reads each line read from csv file (specified by fileAbsolutePath) and parse it into each
     * field as string.
     *
     * @param line read from csv, "data1,data2,data3..."
     * @return: vector containing each field as string, {"data1","data2","data3"...}.
     */
    std::vector<std::string> parseLine(const std::string& line);

    template <CsvFieldType T>
    void appendTo(BSONObjBuilder& builder, const std::string& fieldName, const std::string& data);

    // Reads each line from the CSV file and converts it into a BSONObj, being compliant with the
    // metadata. it will return boost::none if there is no more line to read in the csv file.
    boost::optional<BSONObj> readBsonObj();

    std::string _fileAbsolutePath;
    std::string _metadataAbsolutePath;
    std::ifstream _ifs;
    Metadata _metadata;
    ErrorCount* _errorStats;
};

}  // namespace mongo
