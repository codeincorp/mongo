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

/**
 * This benchmark measures the performance of CsvFileInput when it reads a big csv file.
 */
#include <benchmark/benchmark.h>
#include <filesystem>
#include <sstream>
#include <string>

#include "csv_file.h"

namespace fs = std::filesystem;

namespace mongo {
namespace {

void readCsv(CsvFileInput& input, size_t& totalBytes) {
    input.open();

    constexpr int bufSize = 1000;
    char buf[bufSize];
    while (!input.isEof()) {
        totalBytes += input.read(buf, bufSize);
    }

    input.close();
}

std::string resultStats(const CsvFileIoStats* csvIoStats) {

    std::stringstream sstrm;
    sstrm << "incompleteConversionToNumeric: " << csvIoStats->_incompleteConversionToNumeric
          << '\n';
    sstrm << "invalidInt32: " << csvIoStats->_invalidInt32 << '\n';
    sstrm << "invalidInt64: " << csvIoStats->_invalidInt64 << '\n';
    sstrm << "invalidDouble: " << csvIoStats->_invalidDouble << '\n';
    sstrm << "outOfRange: " << csvIoStats->_outOfRange << '\n';
    sstrm << "invalidDate: " << csvIoStats->_invalidDate << '\n';
    sstrm << "invalidOid: " << csvIoStats->_invalidOid << '\n';
    sstrm << "invalidBoolean: " << csvIoStats->_invalidBoolean << '\n';
    sstrm << "metadataAndDataDifferentLength: " << csvIoStats->_nonCompliantWithMetadata << '\n';
    sstrm << "totalErrorCount: " << csvIoStats->_totalErrorCount << '\n';
    sstrm << "inputSize : " << csvIoStats->_inputSize << '\n';
    sstrm << "outputSize : " << csvIoStats->_outputSize << '\n';
    sstrm << "bsonsReturned : " << csvIoStats->_bsonsReturned << '\n';

    return sstrm.str();
}

void BM_2MillionRecords(benchmark::State& state,
                        const std::string& csvFile,
                        const std::string& metadataFile) {
    system("src/mongo/db/storage/mv_bm_csv.sh");
    CsvFileInput input(csvFile, metadataFile);
    size_t totalBytes = 0;

    for (auto _ : state) {
        readCsv(input, totalBytes);
    }

    std::unique_ptr<CsvFileIoStats> ioStats(
        dynamic_cast<CsvFileIoStats*>(input.releaseIoStats().release()));

    std::cout << resultStats(ioStats.get()) << std::endl;
    fs::path file = "/tmp/" + csvFile;
    uint64_t fileSize = fs::file_size(file);

    state.counters["input_size"] = ioStats->_inputSize;
    state.counters["input_size_per_second"] =
        benchmark::Counter(ioStats->_inputSize, benchmark::Counter::kIsRate);
    state.counters["file_size"] = fileSize;
    state.counters["bson_size"] = totalBytes;
    state.counters["bson_size_per_second"] =
        benchmark::Counter(totalBytes, benchmark::Counter::kIsRate);
    state.SetBytesProcessed(fileSize);
}

BENCHMARK_CAPTURE(BM_2MillionRecords, 2million customers, "customers-2000000.csv", "customers.txt");
BENCHMARK_CAPTURE(BM_2MillionRecords, 2million people, "people-2000000.csv", "people.txt");
BENCHMARK_CAPTURE(BM_2MillionRecords,
                  2million organizations,
                  "organizations-2000000.csv",
                  "organizations.txt");

}  // namespace
}  // namespace mongo
