/**
 * Tests IO stats report from virtual collections.
 */

import {getPlanStage} from "jstests/libs/analyze_plan.js";

const conn = MongoRunner.runMongod();

const db = conn.getDB(jsTestName());
db.dropDatabase();
const coll = db.ext_csv;
const kUrlProtocolFile = "file://";
const kDefaultFilePath = "/tmp/";
const metadataFileName = "error_report.txt";
const metadataFilePath = kDefaultFilePath + metadataFileName;
const metadataFileUrl = kUrlProtocolFile + metadataFileName;
const csvFileName1 = "error_report.csv";
const csvFileName2 = "error_report2.csv";
const csvFileName3 = "error_report3.csv";
const csvFileUrl1 = kUrlProtocolFile + csvFileName1;
const csvFileUrl2 = kUrlProtocolFile + csvFileName2;
const csvFileUrl3 = kUrlProtocolFile + csvFileName3;

// Prepares the metadata file which is common to all data files.
removeFile(metadataFilePath);
copyFile("jstests/noPassthrough/virtual/" + metadataFileName, metadataFilePath);

(function collectEachErrorCases() {
    removeFile(kDefaultFilePath + csvFileName1);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName1, kDefaultFilePath + csvFileName1);

    jsTestLog("Running collectEachErrorCases Test");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: csvFileUrl1, storageType: "file", fileType: "csv"}],
            metadataUrl: metadataFileUrl
        }
    });
    const expectedIoStats = {
        "incompleteConversionToNumeric": NumberLong(4),
        "invalidInt32": NumberLong(1),
        "invalidInt64": NumberLong(1),
        "invalidDouble": NumberLong(2),
        "outOfRange": NumberLong(4),
        "invalidDate": NumberLong(6),
        "invalidOid": NumberLong(5),
        "invalidBoolean": NumberLong(4),
        "metadataAndDataDifferentLength": NumberLong(1),
        "totalErrorCount": NumberLong(28),
        "inputSize": NumberLong(764),
        "outputSize": NumberLong(1036),
        "bsonsReturned": NumberLong(9)
    };
    const res = coll.explain("executionStats")
                    .find({"number": {$gte: 30}}, {kString: 1, number: 1, identifier: 1, signOn: 1})
                    .finish();
    const collScanStage = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, collScanStage);
    assert.eq(expectedIoStats,
              collScanStage.ioStats.csv,
              `Expected ${tojson(expectedIoStats)} but got ${tojson(res)}`);
})();

(function multipleCsvStreams() {
    removeFile(kDefaultFilePath + csvFileName2);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName2, kDefaultFilePath + csvFileName2);
    removeFile(kDefaultFilePath + csvFileName3);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName3, kDefaultFilePath + csvFileName3);

    jsTestLog("Running multipleCsvStreams Test");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: csvFileUrl1, storageType: "file", fileType: "csv"},
                {url: csvFileUrl2, storageType: "file", fileType: "csv"},
                {url: csvFileUrl3, storageType: "file", fileType: "csv"}
            ],
            metadataUrl: metadataFileUrl
        }
    });

    const expectedIoStats = {
        "incompleteConversionToNumeric": NumberLong(6),
        "invalidInt32": NumberLong(5),
        "invalidInt64": NumberLong(4),
        "invalidDouble": NumberLong(7),
        "outOfRange": NumberLong(7),
        "invalidDate": NumberLong(8),
        "invalidOid": NumberLong(8),
        "invalidBoolean": NumberLong(9),
        "metadataAndDataDifferentLength": NumberLong(5),
        "totalErrorCount": NumberLong(59),
        "inputSize": NumberLong(1339),
        "outputSize": NumberLong(1919),
        "bsonsReturned": NumberLong(18)
    };

    const res = coll.explain("executionStats").find().finish();
    const collScanStage = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, collScanStage);
    assert.eq(expectedIoStats,
              collScanStage.ioStats.csv,
              `Expected ${tojson(expectedIoStats)} but got ${tojson(res)}`);
})();

(function multipleMixedTypeStreams() {
    const pipeName1 = "named_pipe1";
    const pipeName2 = "named_pipe2";

    // Prepares the data file
    removeFile(kDefaultFilePath + csvFileName1);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName1, kDefaultFilePath + csvFileName1);

    jsTestLog("Running multipleMixedStreams Test");
    coll.drop();
    // The 'ext_csv' collection will get data from a named pipe and then from a CSV file and then
    // another named pipe.
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: kUrlProtocolFile + pipeName1, storageType: "pipe", fileType: "bson"},
                {url: csvFileUrl1, storageType: "file", fileType: "csv"},
                {url: kUrlProtocolFile + pipeName2, storageType: "pipe", fileType: "bson"},
            ],
            metadataUrl: metadataFileUrl
        }
    });
    // IO stats will only have "csv" section since the named pipe would not cause any errors.
    const expectedIoStats = {
        csv: {
            "incompleteConversionToNumeric": NumberLong(4),
            "invalidInt32": NumberLong(1),
            "invalidInt64": NumberLong(1),
            "invalidDouble": NumberLong(2),
            "outOfRange": NumberLong(4),
            "invalidDate": NumberLong(6),
            "invalidOid": NumberLong(5),
            "invalidBoolean": NumberLong(4),
            "metadataAndDataDifferentLength": NumberLong(1),
            "totalErrorCount": NumberLong(28),
            "inputSize": NumberLong(764),
            "outputSize": NumberLong(1036),
            "bsonsReturned": NumberLong(9)
        }
    };

    // Prepares named pipes.
    const objsPerPipe = 10;
    _writeTestPipeBsonFile(pipeName1,
                           objsPerPipe,
                           "jstests/noPassthrough/external_data_source.bson",
                           kDefaultFilePath);
    _writeTestPipeBsonFile(pipeName2,
                           objsPerPipe,
                           "jstests/noPassthrough/external_data_source.bson",
                           kDefaultFilePath);

    const res = coll.explain("executionStats").find().finish();
    const collScanStage = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, collScanStage);
    assert.eq(expectedIoStats,
              collScanStage
                  .ioStats,  // We examine the whole section of 'ioStats' instead of 'ioStats.csv'.
              `Expected ${tojson(expectedIoStats)} but got ${tojson(res)}`);
})();

(function multipleMixedTypeStreams2() {
    const pipeName = "named_pipe";

    // Prepares the data file
    removeFile(kDefaultFilePath + csvFileName1);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName1, kDefaultFilePath + csvFileName1);
    removeFile(kDefaultFilePath + csvFileName2);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName2, kDefaultFilePath + csvFileName2);
    removeFile(kDefaultFilePath + csvFileName3);
    copyFile("jstests/noPassthrough/virtual/" + csvFileName3, kDefaultFilePath + csvFileName3);

    jsTestLog("Running multipleMixedStreams2 Test");
    coll.drop();
    // The 'ext_csv' collection will get data from both a named pipe and multiple CSV files.
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: csvFileUrl1, storageType: "file", fileType: "csv"},
                {url: csvFileUrl2, storageType: "file", fileType: "csv"},
                {url: kUrlProtocolFile + pipeName, storageType: "pipe", fileType: "bson"},
                {url: csvFileUrl3, storageType: "file", fileType: "csv"}
            ],
            metadataUrl: metadataFileUrl
        }
    });
    // IO stats will only have "csv" section since the named pipe would not cause any errors.
    const expectedIoStats = {
        csv: {
            "incompleteConversionToNumeric": NumberLong(6),
            "invalidInt32": NumberLong(5),
            "invalidInt64": NumberLong(4),
            "invalidDouble": NumberLong(7),
            "outOfRange": NumberLong(7),
            "invalidDate": NumberLong(8),
            "invalidOid": NumberLong(8),
            "invalidBoolean": NumberLong(9),
            "metadataAndDataDifferentLength": NumberLong(5),
            "totalErrorCount": NumberLong(59),
            "inputSize": NumberLong(1339),
            "outputSize": NumberLong(1919),
            "bsonsReturned": NumberLong(18)
        }
    };

    // Prepares named pipes.
    const objsPerPipe = 10;
    _writeTestPipeBsonFile(
        pipeName, objsPerPipe, "jstests/noPassthrough/external_data_source.bson", kDefaultFilePath);

    const res = coll.explain("executionStats").find().finish();
    const collScanStage = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, collScanStage);
    assert.eq(expectedIoStats,
              collScanStage
                  .ioStats,  // We examine the whole section of 'ioStats' instead of 'ioStats.csv'.
              `Expected ${tojson(expectedIoStats)} but got ${tojson(res)}`);
})();

MongoRunner.stopMongod(conn);
