/**
 *    Copyright (C) 2022-present MongoDB, Inc.
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

#ifndef _WIN32
#include <fstream>
#else
#include <windows.h>
#endif
#include <string>

#include "mongo/db/storage/default_path.h"
#include "mongo/db/storage/input_object.h"

namespace mongo {

class NamedPipeOutput {
public:
    // Searches the named pipe in 'kDefaultFilePath' + 'pipeRelativePath'
    NamedPipeOutput(const std::string& pipeRelativePath)
        : NamedPipeOutput(kDefaultFilePath.toString(), pipeRelativePath) {}

    // Searches the named pipe in 'pipeDir' + 'pipeRelativePath' in POSIX system'
    NamedPipeOutput(const std::string& pipeDir,
                    const std::string& pipeRelativePath,
                    bool persistPipe = false);

    ~NamedPipeOutput();
    void open();
    int write(const char* data, int size);
    void close();

private:
    std::string _pipeAbsolutePath;
#ifndef _WIN32
    std::ofstream _ofs;
    bool _persistPipe;
#else
    HANDLE _pipe;
    bool _isOpen;
#endif
};

struct IoStats;

class NamedPipeInput : public StreamableInput {
public:
    NamedPipeInput(const std::string& pipeRelativePath);
    ~NamedPipeInput() override;
    const std::string& getAbsolutePath() const override {
        return _pipeAbsolutePath;
    }
    bool isOpen() const override;
    bool isGood() const override;
    bool isFailed() const override;
    bool isEof() const override;

    // Nothing to return for IoStats.
    std::unique_ptr<IoStats> extractIoStatsSnapshot() {
        return nullptr;
    }

    // Nothing to return for IoStats.
    std::unique_ptr<IoStats> releaseIoStats() {
        return nullptr;
    }

protected:
    void doOpen() override;
    int doRead(char* data, int size) override;
    void doClose() override;

private:
    std::string _pipeAbsolutePath;
#ifndef _WIN32
    std::ifstream _ifs;
#else
    HANDLE _pipe;
    bool _isOpen : 1;
    bool _isGood : 1;
    bool _isEof : 1;
#endif
};
}  // namespace mongo
