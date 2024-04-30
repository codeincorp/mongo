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

#pragma once

#include <cstdint>

namespace mongo {
struct ErrorCount {
    // Variables to keep track of the count of errors occured during reading csv file.
    int64_t _incompleteConversionToNumeric = 0;
    int64_t _invalidInt32 = 0;
    int64_t _invalidInt64 = 0;
    int64_t _invalidDouble = 0;
    int64_t _outOfRange = 0;
    int64_t _invalidDate = 0;
    int64_t _invalidOid = 0;
    int64_t _invalidBoolean = 0;
    int64_t _nonCompliantWithMetadata = 0;
    int64_t _totalErrorCount = 0;

    ErrorCount operator+(const ErrorCount& other) const {
        ErrorCount ret(*this);
        return ret += other;
    }

    ErrorCount& operator+=(const ErrorCount& other) {
        _incompleteConversionToNumeric += other._incompleteConversionToNumeric;
        _invalidInt32 += other._invalidInt32;
        _invalidInt64 += other._invalidInt64;
        _invalidDouble += other._invalidDouble;
        _invalidBoolean += other._invalidBoolean;
        _invalidDate += other._invalidDate;
        _invalidOid += other._invalidOid;
        _outOfRange += other._outOfRange;
        _nonCompliantWithMetadata += other._nonCompliantWithMetadata;
        _totalErrorCount += other._totalErrorCount;
        return *this;
    }

    void incIncompleteConversionToNumeric() {
        _incompleteConversionToNumeric++;
        _totalErrorCount++;
    }

    void incInvalidInt32() {
        _invalidInt32++;
        _totalErrorCount++;
    }

    void incInvalidInt64() {
        _invalidInt64++;
        _totalErrorCount++;
    }

    void incInvalidDouble() {
        _invalidDouble++;
        _totalErrorCount++;
    }

    void incInvalidBoolean() {
        _invalidBoolean++;
        _totalErrorCount++;
    }

    void incInvalidOid() {
        _invalidOid++;
        _totalErrorCount++;
    }

    void incInvalidDate() {
        _invalidDate++;
        _totalErrorCount++;
    }

    void incNonCompliantWithMetadata() {
        _nonCompliantWithMetadata++;
        _totalErrorCount++;
    }

    void incOutOfRange() {
        _outOfRange++;
        _totalErrorCount++;
    }
};
}  // namespace mongo
