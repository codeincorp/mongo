/**
 * Tests virtual collections over external CSV file(s).
 *
 * @tags: [
 * # This test file requires multi-threading for writers and tends to fail on small machines due to
 * # thread resource shortage
 * requires_external_data_source
 * ]
 */

const conn = MongoRunner.runMongod();

const db = conn.getDB(jsTestName());
db.dropDatabase();

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

const coll = db.ext_csv;

(function testBasicCsvFile() {
    jsTestLog("Running testBasicCsvFile()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    const expected = [kMaryMiller, kJohnWilliam, kJamesRobert];
    const res = coll.find().toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testBasicCsvFileWithMetadata() {
    jsTestLog("Running testBasicCsvFileWithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadata: {
                firstName: "string",
                lastName: "string",
                age: "int",
                subscriptionDate: "date",
                retired: "bool"
            }
        }
    });

    const expected = [kMaryMiller, kJohnWilliam, kJamesRobert];
    const res = coll.find().toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testDoubleCsvFiles() {
    jsTestLog("Running testDoubleCsvFiles()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: "file://test.csv", storageType: "file", fileType: "csv"},
                {url: "file://test.csv", storageType: "file", fileType: "csv"}
            ],
            metadataUrl: "file://test.txt"
        }
    });

    const allDocs = [kMaryMiller, kJohnWilliam, kJamesRobert];
    const expected = allDocs.concat(allDocs);
    const res = coll.find().toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testDoubleCsvFilesWithMetadata() {
    jsTestLog("Running testDoubleCsvFilesWithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
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
            }
        }
    });

    const allDocs = [kMaryMiller, kJohnWilliam, kJamesRobert];
    const expected = allDocs.concat(allDocs);
    const res = coll.find().toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testFilterSanity() {
    jsTestLog("Running testFilterSanity()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    const expected = [kJohnWilliam, kJamesRobert];
    const res = coll.find({age: {$gt: 10}}).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testFilterSanityWithMetadata() {
    jsTestLog("Running testFilterSanityWithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadata: {
                firstName: "string",
                lastName: "string",
                age: "int",
                subscriptionDate: "date",
                retired: "bool"
            }
        }
    });

    const expected = [kJohnWilliam, kJamesRobert];
    const res = coll.find({age: {$gt: 10}}).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.eq(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testGroupSanity1() {
    jsTestLog("Running testGroupSanity1()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    const expected = [{_id: false, c: 1}, {_id: true, c: 2}];
    const res = coll.aggregate([{$group: {_id: "$retired", c: {$sum: 1}}}]).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testGroupSanity1WithMetadata() {
    jsTestLog("Running testGroupSanity1WithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadata: {
                firstName: "string",
                lastName: "string",
                age: "int",
                subscriptionDate: "date",
                retired: "bool"
            }
        }
    });

    const expected = [{_id: false, c: 1}, {_id: true, c: 2}];
    const res = coll.aggregate([{$group: {_id: "$retired", c: {$sum: 1}}}]).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testGroupSanity2() {
    jsTestLog("Running testGroupSanity2()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [
                {url: "file://test.csv", storageType: "file", fileType: "csv"},
                {url: "file://test.csv", storageType: "file", fileType: "csv"},
                {url: "file://test.csv", storageType: "file", fileType: "csv"}
            ],
            metadataUrl: "file://test.txt"
        }
    });

    const expected =
        [{_id: "Mary Miller", c: 3}, {_id: "John William", c: 3}, {_id: "James Robert", c: 3}];
    const res =
        coll.aggregate([{$group: {_id: {$concat: ["$firstName", " ", "$lastName"]}, c: {$sum: 1}}}])
            .toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testGroupSanity2Metadata() {
    jsTestLog("Running testGroupSanity2Metadata()");
    coll.drop();
    db.createCollection("ext_csv", {
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
            }
        }
    });

    const expected =
        [{_id: "Mary Miller", c: 3}, {_id: "John William", c: 3}, {_id: "James Robert", c: 3}];
    const res =
        coll.aggregate([{$group: {_id: {$concat: ["$firstName", " ", "$lastName"]}, c: {$sum: 1}}}])
            .toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testSortSanity() {
    jsTestLog("Running testSortSanity()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    const expected = [kJamesRobert, kMaryMiller, kJohnWilliam];
    const res = coll.find({}, {}, {sort: {subscriptionDate: -1}}).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testSortSanityWithMetadata() {
    jsTestLog("Running testSortSanityWithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadata: {
                firstName: "string",
                lastName: "string",
                age: "int",
                subscriptionDate: "date",
                retired: "bool"
            }
        }
    });

    const expected = [kJamesRobert, kMaryMiller, kJohnWilliam];
    const res = coll.find({}, {}, {sort: {subscriptionDate: -1}}).toArray();
    assert.eq(res.length, expected.length, `Expected ${tojson(res)} but got ${tojson(expected)}`);
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testSortLimitSanity() {
    jsTestLog("Running testSortLimitSanity()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    const expected = [kMaryMiller];
    const res = coll.find({}, {}, {sort: {age: 1}}).limit(1).toArray();
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testSortLimitSanityWithMetadata() {
    jsTestLog("Running testSortLimitSanityWithMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadata: {
                firstName: "string",
                lastName: "string",
                age: "int",
                subscriptionDate: "date",
                retired: "bool"
            }
        }
    });

    const expected = [kMaryMiller];
    const res = coll.find({}, {}, {sort: {age: 1}}).limit(1).toArray();
    assert.sameMembers(res, expected, `Expected ${tojson(res)} but got ${tojson(expected)}`);
})();

(function testUnsupportedFileType() {
    jsTestLog("Running testUnsupportedFileType()");
    coll.drop();
    assert.commandFailedWithCode(db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "yml"}],
            metadataUrl: "file://test.txt"
        }
    }),
                                 ErrorCodes.BadValue);
})();

(function testUnsupportedStorageType() {
    jsTestLog("Running testUnsupportedStorageType()");
    coll.drop();
    assert.commandFailedWithCode(db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "block", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    }),
                                 ErrorCodes.BadValue);
})();

(function testNonExistentCsvFile() {
    jsTestLog("Running testNonExistentCsvFile()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test1.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test.txt"
        }
    });

    assert.throwsWithCode(() => {
        coll.find().toArray();
    }, ErrorCodes.FileNotOpen);
})();

(function testNonExistentMetadata() {
    jsTestLog("Running testNonExistentMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
            metadataUrl: "file://test1.txt"
        }
    });

    assert.throwsWithCode(() => {
        coll.find().toArray();
    }, ErrorCodes.FileNotOpen);
})();

(function testMissingMetadata() {
    jsTestLog("Running testNonExistentMetadata()");
    coll.drop();
    db.createCollection("ext_csv", {
        virtual: {
            dataSources: [{url: "file://test.csv", storageType: "file", fileType: "csv"}],
        }
    });

    assert.throwsWithCode(() => {
        coll.find().toArray();
    }, 200000600);
})();

MongoRunner.stopMongod(conn);
