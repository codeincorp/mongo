/**
 * Tests virtual collections over external CSV file.
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

const expected = [
    {
        "firstName": "Mary",
        "lastName": "Miller",
        "age": 10,
        "subscriptionDate": ISODate("2023-01-23T00:00:00Z"),
        "retired": false
    },
    {
        "firstName": "John",
        "lastName": "William",
        "age": 65,
        "subscriptionDate": ISODate("2013-07-15T00:00:00Z"),
        "retired": true
    },
    {
        "firstName": "James",
        "lastName": "Robert",
        "age": 90,
        "subscriptionDate": ISODate("2024-03-01T00:00:00Z"),
        "retired": true
    }
];

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
    const res = coll.find().toArray();
    const doubleExpected = expected.concat(expected);
    assert.eq(res.length,
              doubleExpected.length,
              `Expected ${tojson(res)} but got ${tojson(doubleExpected)}`);
    assert.eq(res, doubleExpected, `Expected ${tojson(res)} but got ${tojson(doubleExpected)}`);
})();

(function testNonExistentFile() {
    jsTestLog("Running testNonExistentFile()");
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

MongoRunner.stopMongod(conn);
