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

#include <benchmark/benchmark.h>
#include <filesystem>
#include <string>

#include "csv_file.h"

namespace fs = std::filesystem;

namespace mongo {
namespace {

void readCsv(CsvFileInput& input) {
    constexpr int bufSize = 1000;
    char buf[bufSize];
    while (!input.isEof()) {
        input.read(buf, bufSize);
    }

    input.close();
}

void BM_2MillionRecords(benchmark::State& state,
                        const std::string& csvFile,
                        const std::string& metadataFile) {
    system("src/mongo/db/storage/mv_bm_csv.sh");
    mongo::CsvFileInput input(csvFile, metadataFile);
    input.open();

    for (auto _ : state) {
        readCsv(input);
    }

    fs::path file = "/tmp/" + csvFile;
    uint64_t totalBytes = fs::file_size(file);

    auto errorCount = input.releaseIoStats();
    BSONObjBuilder builder;
    errorCount->appendTo(builder);
    std::cout << builder.done().toString() << std::endl;

    state.SetBytesProcessed(totalBytes);
}

BENCHMARK_CAPTURE(BM_2MillionRecords, 2million customers, "customers-2000000.csv", "customers.txt");
BENCHMARK_CAPTURE(BM_2MillionRecords, 2million people, "people-2000000.csv", "people.txt");
BENCHMARK_CAPTURE(BM_2MillionRecords,
                  2million organizations,
                  "organizations-2000000.csv",
                  "organizations.txt");

}  // namespace
}  // namespace mongo
