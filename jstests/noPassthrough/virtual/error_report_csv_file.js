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

(function test1() {
    removeFile("/tmp/error_report.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report.csv", "/tmp/error_report.csv");

    jsTestLog("Running Test1");
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

(function test2() {
    removeFile("/tmp/error_report2.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report2.csv", "/tmp/error_report2.csv");

    jsTestLog("Running Test2");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://error_report2.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://error_report.txt"
        }
    });

    const expectedRecordStoreStats = {
        "incompleteConversionToNumeric": NumberLong(1),
        "invalidInt32": NumberLong(1),
        "invalidInt64": NumberLong(1),
        "invalidDouble": NumberLong(1),
        "outOfRange": NumberLong(1),
        "invalidDate": NumberLong(1),
        "invalidOid": NumberLong(1),
        "invalidBoolean": NumberLong(1),
        "metadataAndDataDifferentLength": NumberLong(1),
        "totalErrorCount": NumberLong(9)
    };
    const res = coll.explain("executionStats").find().finish();
    const recordStoreStats = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, recordStoreStats);
    assert.eq(expectedRecordStoreStats,
              recordStoreStats.recordStoreStats,
              `Expected ${tojson(expectedRecordStoreStats)} but got ${tojson(res)}`);
})();

(function test3() {
    jsTestLog("Running Test3");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: "file://error_report.csv", storageType: "file", fileType: "csv"},
                {url: "file://error_report2.csv", storageType: "file", fileType: "csv"}
            ],
            metadataUrl: "file://error_report.txt"
        }
    });

    const expectedRecordStoreStats = {
        "incompleteConversionToNumeric": NumberLong(5),
        "invalidInt32": NumberLong(2),
        "invalidInt64": NumberLong(2),
        "invalidDouble": NumberLong(3),
        "outOfRange": NumberLong(5),
        "invalidDate": NumberLong(7),
        "invalidOid": NumberLong(6),
        "invalidBoolean": NumberLong(5),
        "metadataAndDataDifferentLength": NumberLong(2),
        "totalErrorCount": NumberLong(37)
    };

    const res = coll.explain("executionStats").find().finish();
    const recordStoreStats = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, recordStoreStats);
    assert.eq(expectedRecordStoreStats,
              recordStoreStats.recordStoreStats,
              `Expected ${tojson(expectedRecordStoreStats)} but got ${tojson(res)}`);
})();

(function test4() {
    removeFile("/tmp/error_report3.csv");
    copyFile(pwd() + "/jstests/noPassthrough/virtual/error_report3.csv", "/tmp/error_report3.csv");

    jsTestLog("Running allErrorTest");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://error_report3.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://error_report.txt"
        }
    });

    const expectedRecordStoreStats = {
        "incompleteConversionToNumeric": NumberLong(1),
        "invalidInt32": NumberLong(3),
        "invalidInt64": NumberLong(2),
        "invalidDouble": NumberLong(4),
        "outOfRange": NumberLong(2),
        "invalidDate": NumberLong(1),
        "invalidOid": NumberLong(2),
        "invalidBoolean": NumberLong(4),
        "metadataAndDataDifferentLength": NumberLong(3),
        "totalErrorCount": NumberLong(22)
    };

    const res = coll.explain("executionStats").find().finish();
    const recordStoreStats = getPlanStage(res.executionStats.executionStages, "COLLSCAN");
    assert.neq(null, recordStoreStats);
    assert.eq(expectedRecordStoreStats,
              recordStoreStats.recordStoreStats,
              `Expected ${tojson(expectedRecordStoreStats)} but got ${tojson(res)}`);
})();

MongoRunner.stopMongod(conn);
