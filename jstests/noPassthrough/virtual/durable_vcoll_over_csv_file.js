/**
 * Tests durable virtual collections over external CSV file(s).
 *
 * @tags: [
 * # This test file requires multi-threading for writers and tends to fail on small machines due to
 * # thread resource shortage
 * requires_external_data_source
 * ]
 */

import {getCallerName} from "jstests/core/timeseries/libs/timeseries_writes_util.js";

const kMaryMiller = {
    "firstName": "Mary",
    "lastName": "Miller",
    "age": 10,
    "subscriptionDate": ISODate("2023-01-23T00:00:00Z"),
    "retired": false
};
const kJohnWilliam = {
    "firstName": "John",
    "lastName": "William",
    "age": 65,
    "subscriptionDate": ISODate("2013-07-15T00:00:00Z"),
    "retired": true
};
const kJamesRobert = {
    "firstName": "James",
    "lastName": "Robert",
    "age": 90,
    "subscriptionDate": ISODate("2024-03-01T00:00:00Z"),
    "retired": true
};

removeFile("/tmp/test.csv");
copyFile(pwd() + "/jstests/noPassthrough/virtual/test.csv", "/tmp/test.csv");
removeFile("/tmp/test.txt");
copyFile(pwd() + "/jstests/noPassthrough/virtual/test.txt", "/tmp/test.txt");

function runTest({vopts, verifyFn}) {
    const testCaseName = getCallerName();
    jsTestLog(`Running ${testCaseName}`);

    jsTestLog(`${testCaseName}: Initial creation of durable virtual collection`);
    let conn = MongoRunner.runMongod();

    let db = conn.getDB(jsTestName());
    db.dropDatabase();
    let coll = db.durable_vcoll;
    coll.drop();

    db.createCollection("durable_vcoll", vopts);
    verifyFn(coll);

    MongoRunner.stopMongod(conn, null, {skipValidation: true});

    jsTestLog(`${
        testCaseName}: Checking whether the durable virtual collection survives a server restart`);
    conn = MongoRunner.runMongod({restart: conn, noCleanData: true});

    db = conn.getDB(jsTestName());
    coll = db.durable_vcoll;
    verifyFn(coll);
    assert(coll.drop());

    MongoRunner.stopMongod(conn, null, {skipValidation: true});

    jsTestLog(`${
        testCaseName
    }: Checking if the durable virtual collection is dropped from the durable catalog`);
    conn = MongoRunner.runMongod({restart: conn, noCleanData: true});
    db = conn.getDB(jsTestName());
    const res = db.getCollectionNames();
    assert.eq(res.length, 0, `Expected 0 collections but got ${tojson(res)}`);

    MongoRunner.stopMongod(conn, null, {skipValidation: true});
}

(function testBasicCsvFile() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kMaryMiller, kJohnWilliam, kJamesRobert];
            const res = coll.find().toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testBasicCsvFileWithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kMaryMiller, kJohnWilliam, kJamesRobert];
            const res = coll.find().toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testDoubleCsvFiles() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"}
                ],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const allDocs = [kMaryMiller, kJohnWilliam, kJamesRobert];
            const expected = allDocs.concat(allDocs);
            const res = coll.find().toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testDoubleCsvFilesWithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"}
                ],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }

        },
        verifyFn: (coll) => {
            const allDocs = [kMaryMiller, kJohnWilliam, kJamesRobert];
            const expected = allDocs.concat(allDocs);
            const res = coll.find().toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testFilterSanity() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kJohnWilliam, kJamesRobert];
            const res = coll.find({age: {$gt: 10}}).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testFilterSanityWithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kJohnWilliam, kJamesRobert];
            const res = coll.find({age: {$gt: 10}}).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.eq(res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testGroupSanity1() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [{_id: false, c: 1}, {_id: true, c: 2}];
            const res = coll.aggregate([{$group: {_id: "$retired", c: {$sum: 1}}}]).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testGroupSanity1WithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [{_id: false, c: 1}, {_id: true, c: 2}];
            const res = coll.aggregate([{$group: {_id: "$retired", c: {$sum: 1}}}]).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testGroupSanity2() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"}
                ],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [
                {_id: "Mary Miller", c: 3},
                {_id: "John William", c: 3},
                {_id: "James Robert", c: 3}
            ];
            const res =
                coll.aggregate([
                        {$group: {_id: {$concat: ["$firstName", " ", "$lastName"]}, c: {$sum: 1}}}
                    ])
                    .toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testGroupSanity2Metadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"},
                    {url: "file://test.csv", storageType: "file", fileType: "csv"}
                ],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [
                {_id: "Mary Miller", c: 3},
                {_id: "John William", c: 3},
                {_id: "James Robert", c: 3}
            ];
            const res =
                coll.aggregate([
                        {$group: {_id: {$concat: ["$firstName", " ", "$lastName"]}, c: {$sum: 1}}}
                    ])
                    .toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testSortSanity() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kJamesRobert, kMaryMiller, kJohnWilliam];
            const res = coll.find({}, {}, {sort: {subscriptionDate: -1}}).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testSortSanityWithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kJamesRobert, kMaryMiller, kJohnWilliam];
            const res = coll.find({}, {}, {sort: {subscriptionDate: -1}}).toArray();
            assert.eq(
                res.length, expected.length, `Expected ${tojson(expected)} but got ${tojson(res)}`);
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testSortLimitSanity() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kMaryMiller];
            const res = coll.find({}, {}, {sort: {age: 1}}).limit(1).toArray();
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testSortLimitSanityWithMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadata: {
                    firstName: "string",
                    lastName: "string",
                    age: "int",
                    subscriptionDate: "date",
                    retired: "bool"
                },
                durable: true
            }
        },
        verifyFn: (coll) => {
            const expected = [kMaryMiller];
            const res = coll.find({}, {}, {sort: {age: 1}}).limit(1).toArray();
            assert.sameMembers(
                res, expected, `Expected ${tojson(expected)} but got ${tojson(res)}`);
        }
    });
})();

(function testNonExistentCsvFile() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test1.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            assert.throwsWithCode(() => {
                coll.find().toArray();
            }, ErrorCodes.FileNotOpen);
        }
    });
})();

(function testNonExistentMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                metadataUrl: "file://test1.txt",
                durable: true
            }
        },
        verifyFn: (coll) => {
            assert.throwsWithCode(() => {
                coll.find().toArray();
            }, ErrorCodes.FileNotOpen);
        }
    });
})();

(function testMissingMetadata() {
    runTest({
        vopts: {
            virtual: {
                dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
                durable: true
            }
        },
        verifyFn: (coll) => {
            assert.throwsWithCode(() => {
                coll.find().toArray();
            }, 200000600);
        }
    });
})();
