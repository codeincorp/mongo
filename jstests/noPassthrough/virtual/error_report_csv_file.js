/**
 * Tests error report over virtual collections over external CSV file(s).
 */

import {
    getPlanStage,
} from "jstests/libs/analyze_plan.js";

removeFile("/tmp/error_report.txt");
copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report.txt", "/tmp/error_report.txt");

const conn = MongoRunner.runMongod();

const db = conn.getDB(jsTestName());
db.dropDatabase();

const coll = db.ext_csv;

(function collectEachErrorCases() {
    removeFile("/tmp/error_report.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report.csv", "/tmp/error_report.csv");

    jsTestLog("Running CollectEachErrorCases Test");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://error_report.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://error_report.txt"
        }
    });
    const expectedRecordStoreStats = {
        "incompleteConversionToNumeric": NumberLong(4),
        "invalidInt32": NumberLong(1),
        "invalidInt64": NumberLong(1),
        "invalidDouble": NumberLong(2),
        "outOfRange": NumberLong(4),
        "invalidDate": NumberLong(6),
        "invalidOid": NumberLong(5),
        "invalidBoolean": NumberLong(4),
        "metadataAndDataDifferentLength": NumberLong(1),
        "totalErrorCount": NumberLong(28)
    };
    const res = coll.explain("executionStats")
                    .find({"number": {$gte: 30}}, {kString: 1, number: 1, identifier: 1, signOn: 1})
                    .finish();
    const recordStoreStats = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, recordStoreStats);
    assert.eq(expectedRecordStoreStats,
              recordStoreStats.recordStoreStats,
              `Expected ${tojson(expectedRecordStoreStats)} but got ${tojson(res)}`);
})();

(function multipleCsvStreams() {
    removeFile("/tmp/error_report2.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report2.csv", "/tmp/error_report2.csv");
    removeFile("/tmp/error_report3.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report3.csv", "/tmp/error_report3.csv");

    jsTestLog("Running MultipleCsvStreams Test");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: "file://error_report.csv", storageType: "file", fileType: "csv"},
                {url: "file://error_report2.csv", storageType: "file", fileType: "csv"},
                {url: "file://error_report3.csv", storageType: "file", fileType: "csv"}
            ],
            metadataUrl: "file://error_report.txt"
        }
    });

    const expectedRecordStoreStats = {
        "incompleteConversionToNumeric": NumberLong(6),
        "invalidInt32": NumberLong(5),
        "invalidInt64": NumberLong(4),
        "invalidDouble": NumberLong(7),
        "outOfRange": NumberLong(7),
        "invalidDate": NumberLong(8),
        "invalidOid": NumberLong(8),
        "invalidBoolean": NumberLong(9),
        "metadataAndDataDifferentLength": NumberLong(5),
        "totalErrorCount": NumberLong(59)
    };

    const res = coll.explain("executionStats").find().finish();
    const recordStoreStats = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, recordStoreStats);
    assert.eq(expectedRecordStoreStats,
              recordStoreStats.recordStoreStats,
              `Expected ${tojson(expectedRecordStoreStats)} but got ${tojson(res)}`);
})();

MongoRunner.stopMongod(conn);
