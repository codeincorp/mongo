#pragma once

#include <cstdint>

namespace mongo {
struct ErrorCount {
    // Variables to keep track of the count of errors occured during reading csv file.
    std::int64_t _incompleteConversionToNumeric = 0;
    std::int64_t _invalidInt32 = 0;
    std::int64_t _invalidInt64 = 0;
    std::int64_t _invalidDouble = 0;
    std::int64_t _outOfRange = 0;
    std::int64_t _invalidDate = 0;
    std::int64_t _invalidOid = 0;
    std::int64_t _invalidBoolean = 0;
    std::int64_t _metadataAndDataDifferentLength = 0;
    std::int64_t _totalErrorCount = 0;

    ErrorCount operator+(const ErrorCount& other) const {
        ErrorCount ret;
        ret._incompleteConversionToNumeric =
            this->_incompleteConversionToNumeric + other._incompleteConversionToNumeric;
        ret._invalidInt32 = this->_invalidInt32 + other._invalidInt32;
        ret._invalidInt64 = this->_invalidInt64 + other._invalidInt64;
        ret._invalidDouble = this->_invalidDouble + other._invalidDouble;
        ret._outOfRange = this->_outOfRange + other._outOfRange;
        ret._invalidDate = this->_invalidDate + other._invalidDate;
        ret._invalidBoolean = this->_invalidBoolean + other._invalidBoolean;
        ret._invalidOid = this->_invalidOid + other._invalidOid;
        ret._metadataAndDataDifferentLength =
            this->_metadataAndDataDifferentLength + other._metadataAndDataDifferentLength;
        ret._totalErrorCount = this->_totalErrorCount + other._totalErrorCount;
        return ret;
    }

    ErrorCount& operator+=(const ErrorCount& other) {
        this->_incompleteConversionToNumeric += other._incompleteConversionToNumeric;
        this->_invalidInt32 += other._invalidInt32;
        this->_invalidInt64 += other._invalidInt64;
        this->_invalidDouble += other._invalidDouble;
        this->_invalidBoolean += other._invalidBoolean;
        this->_invalidDate += other._invalidDate;
        this->_invalidOid += other._invalidOid;
        this->_outOfRange += other._outOfRange;
        this->_metadataAndDataDifferentLength += other._metadataAndDataDifferentLength;
        this->_totalErrorCount += other._totalErrorCount;
        return *this;
    }
};
}  // namespace mongo
