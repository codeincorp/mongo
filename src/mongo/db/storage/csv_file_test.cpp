#include <unistd.h>
#include <vector>

#include "csv_file.h"
#include "mongo/bson/json.h"
#include "mongo/unittest/assert.h"

namespace mongo {

using namespace fmt::literals;

class CsvFileInputTest : public unittest::Test {
protected:
    void setUp() override {
        system("cp -rn src/mongo/db/storage/csv_test /tmp/");
    }
};

TEST_F(CsvFileInputTest, CsvBasicRead) {
    CsvFileInput input("csv_test/basicRead.csv", "csv_test/basicRead.txt");

    std::vector expected = {

        fromjson(R"(
{
    field1: 12,
    boolean: true,
    decimal: 3.12345678901234522,
    textField: "string",
    docIdentifier: ObjectId("66075df233ce5deb424257fb"),
    moment: {$date: "2013-07-23T11:42:14.072Z"},
    billionaire: 150000000000
})"),
        fromjson(R"(
{
    field1: 13, 
    boolean: true, 
    decimal: 1.2, 
    textField: "Plummer", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"2017-08-06T13:13:59.010+07:00"},
    billionaire: 120000000000
})"),
        fromjson(R"(
{
    field1: 14, 
    boolean: false, 
    decimal: 5.5, 
    textField: "Chair", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment: {$date:"2019-10-23T21:42:14.144Z"},
    billionaire: 135000000000
})"),
        fromjson(R"(
{
    field1: 16, 
    boolean: true, 
    decimal: 6.6, 
    textField: "Bottle", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment: {$date:"2016-11-11T01:16:23.543Z"},
    billionaire: 2000000000000
})"),
        fromjson(R"(
{
    field1: 21, 
    boolean: false, 
    decimal: 0.9, 
    textField: "Tesla", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"2017-10-31T08:23:49.982Z"},
    billionaire: 100000000000000
})"),
        fromjson(R"(
{
    field1: 15, 
    boolean: true, 
    decimal: 3.3, 
    textField: "Notebook", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment: {$date:"2012-12-23T23:59:54.932Z"},
    billionaire: 53000000000
})"),
        fromjson(R"(
{
    field1: 18, 
    boolean: true, 
    decimal: 9.1, 
    textField: "JSON", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"1999-04-25T09:37:09.883Z"},
    billionaire: 9000000000
})"),
        fromjson(R"(
{
    field1: 21, 
    boolean: false, 
    decimal: 3.98, 
    textField: "BSON", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"1970-03-28T07:34:42.390Z"},
    billionaire: 70000000000
})"),
        fromjson(R"(
{ 
    field1: 31, 
    boolean: true, 
    decimal: 2.09, 
    textField: "Testosterone", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment: {$date:"2024-03-23T21:21:55.559Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 27, 
    boolean: false, 
    decimal: 12.34, 
    textField: "Chipmunk", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"2023-12-30T12:12:14.645Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 41, 
    boolean: false, 
    decimal: 91.2, 
    textField: "Table", 
    docIdentifier: ObjectId("66075df233ce5deb424257fb"), 
    moment:{$date:"2020-08-23T13:17:39.345Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 52, 
    boolean: false, 
    decimal: 93.2, 
    textField: "Bravo", 
    docIdentifier: ObjectId("660a04700ea7913a8fced3f4"), 
    moment: {$date:"2013-07-03T03:23:23.900+05:00"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 91, 
    boolean: true, 
    decimal: 0.24, 
    textField: "Beethoven", 
    docIdentifier: ObjectId("660a048f0ea7913a8fced3f6"), 
    moment: {$date:"2006-12-12T12:38:48.985-04:00"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 123, 
    boolean: true, 
    decimal: 0.111, 
    textField: "Hikaru",
    docIdentifier: ObjectId("660a04910ea7913a8fced3f8"), 
    moment: {$date:"2004-04-04T19:07:21.388-02:00"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 912, 
    boolean: true, 
    decimal: 1.231, 
    textField: "Spinal Chord", 
    docIdentifier: ObjectId("660a04920ea7913a8fced3fa"), 
    moment: {$date:"2010-11-11T21:21:59.991Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 1023, 
    boolean: false, 
    decimal: 5.121, 
    textField: "Large and Powerful", 
    docIdentifier: ObjectId("660a04930ea7913a8fced3fc"), 
    moment: {$date:"2011-09-09T13:29:31.211-06:00"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 34, 
    boolean: true, 
    decimal: 0.123, 
    textField: "Arresto Momentum", 
    docIdentifier: ObjectId("660a04940ea7913a8fced3fe"), 
    moment: {$date:"2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 102,
    boolean: false,
    decimal: 0.123,
    textField: "SheldonCooper",
    docIdentifier: ObjectId("66abcf940ea793f3dfceecae"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77111123456
})"),
        fromjson(R"(
{
    field1: 2020,
    boolean: false,
    decimal: 0.123,
    textField: "ObjectiveC",
    docIdentifier: ObjectId("ffea04940ee7f19ab8efa1fc"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 34,
    boolean: true,
    decimal: 0.123,
    textField: "CLOWN",
    docIdentifier: ObjectId("abcdef941ea39e3781c0dcfe"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 23,
    boolean: false,
    decimal: 0.123,
    textField: "Linux is better than Mac",
    docIdentifier: ObjectId("abcdef941ea39e3781c0dcfe"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 611,
    boolean: true,
    decimal: 0.123,
    textField: "APPLE",
    docIdentifier: ObjectId("abcdef941ea39e3781c0dcfe"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),

        fromjson(R"(
{
    field1: 63,
    boolean: true,
    decimal: -9223372036854774784,
    textField: "IstanU",
    docIdentifier: ObjectId("12abc6edf01aab5ff8d0feca"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})"),
        fromjson(R"(
{
    field1: 93,
    boolean: false,
    decimal: 9223372036854773760,
    textField: "Arresto Momentum",
    docIdentifier: ObjectId("19ec449399a7cbadffcff3fe"),
    moment: {$date: "2018-01-30T23:00:01.009Z"},
    billionaire: 77000000000
})")};

    input.open();
    ASSERT_TRUE(input.isOpen());

    int bufSize = 200;
    char buf[bufSize];
    int nRead = 0;
    int line = 0;
    do {
        ASSERT(input.isGood());
        ASSERT(!input.isEof());
        ASSERT(!input.isFailed());
        nRead = input.read(buf, bufSize);

        BSONObj obj(buf);
        if (nRead > 0) {
            ASSERT_BSONOBJ_EQ(obj, expected[line]);
        }
        line++;
    } while (nRead != 0);

    input.close();
    ASSERT(!input.isOpen());
}

TEST_F(CsvFileInputTest, AbsentField) {
    std::vector expected = {

        fromjson(R"(
{
    things: "HedgeFund",
    when: {$date: "2020-10-10T10:10:10.101-06:00"}, 
    count: null,
    long: 9000000000, 
    identifier: ObjectId("123456789a123456789b1fcb"),
    correct: true
})"),
        fromjson(R"(
{
    things: null,
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: 56,
    long: 2300000000, 
    identifier: ObjectId("a987654321bdbbcbeebfb528"),
    correct: true
})"),
        fromjson(R"(
{
    things: "Schema", 
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: null,
    long: 4000000000,
    identifier: null,
    correct: false
})"),
        fromjson(R"(
{
    things: "field1",
    when: null,
    count: 45,
    long: null, 
    identifier: null,
    correct: true
})"),
        fromjson(R"(
{
    things: "field3",
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: 46,
    long: 5000000000,
    identifier: ObjectId("1234567890abcdefabcd12ef"),
    correct: true
})"),
        fromjson(R"(
{
    things: "Texting", 
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: 47,
    long: null, 
    identifier: null,
    correct: false
})"),
        fromjson(R"(
{
    things: "phone",
    when: null,
    count: 48,
    long: 9876543210,
    identifier: ObjectId("123456789012345678901234"),
    correct: null
})"),
        fromjson(R"(
{
    things: "Hello World",
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: null,
    long: 98765432123,
    identifier: null,
    correct: true
})"),
        fromjson(R"(
{
    things: "ipad",
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: 49,
    long: null,
    identifier: ObjectId("12345678901234567890aaaa"),
    correct: false
})"),
        fromjson(R"(
{
    things: "remote", 
    when: {$date: "2020-10-10T10:10:10.101-06:00"},
    count: 50,
    long: 77777777776,
    identifier: null,
    correct: null
})"),
        fromjson(R"(
{
    things: "controller", 
    when: null,
    count: null,
    long: 44444444444,
    identifier: null,
    correct: false
})")};

    CsvFileInput input("csv_test/absentField.csv", "csv_test/absentField.txt");
    input.open();

    int bufSize = 250;
    char buf[bufSize];

    for (int i = 0; i < 11; i++) {
        input.read(buf, bufSize);
        ASSERT_BSONOBJ_EQ(BSONObj(buf), expected[i]);
    }

    input.close();
}

TEST_F(CsvFileInputTest, FailByBadOID) {
    CsvFileInput input("csv_test/badOid.csv", "csv_test/badOid.txt");
    input.open();

    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 9; i++) {
        ASSERT_THROWS(input.read(buf, bufSize), DBException);
    }
    input.close();
}

TEST_F(CsvFileInputTest, FailByBadInt) {
    CsvFileInput badInt("csv_test/badInt.csv", "csv_test/badInt.txt");
    badInt.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 6; i++) {
        ASSERT_THROWS_CODE(badInt.read(buf, bufSize), DBException, ErrorCodes::BadValue);
    }
    badInt.close();
}

TEST_F(CsvFileInputTest, FailByBadDate) {
    CsvFileInput badDate("csv_test/badDate.csv", "csv_test/badDate.txt");
    badDate.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 4; i++) {
        ASSERT_THROWS_CODE(badDate.read(buf, bufSize), DBException, ErrorCodes::BadValue);
    }
    badDate.close();
}

TEST_F(CsvFileInputTest, FailByBadLong) {
    CsvFileInput badLong("csv_test/badLong.csv", "csv_test/badLong.txt");
    badLong.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 5; i++) {
        ASSERT_THROWS_CODE(badLong.read(buf, bufSize), DBException, ErrorCodes::BadValue);
    }
    badLong.close();
}

TEST_F(CsvFileInputTest, FailByBadBoolean) {
    CsvFileInput badBoolean("csv_test/badBoolean.csv", "csv_test/badBoolean.txt");
    badBoolean.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 11; i++) {
        ASSERT_THROWS_CODE(badBoolean.read(buf, bufSize), DBException, ErrorCodes::BadValue);
    }
    badBoolean.close();
}

TEST_F(CsvFileInputTest, FailByBadDecimal) {
    CsvFileInput badDecimal("csv_test/badDecimal.csv", "csv_test/badDecimal.txt");
    badDecimal.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 4; i++) {
        ASSERT_THROWS_CODE(badDecimal.read(buf, bufSize), DBException, ErrorCodes::BadValue);
    }
    badDecimal.close();
}

TEST_F(CsvFileInputTest, FailByOutOfRange) {
    CsvFileInput intOverflow("csv_test/intOutOfRange.csv", "csv_test/intOutOfRange.txt");
    intOverflow.open();
    int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 6; i++) {
        ASSERT_THROWS_CODE(intOverflow.read(buf, bufSize), DBException, ErrorCodes::Overflow);
    }
    intOverflow.close();

    CsvFileInput longOverflow("csv_test/longOutOfRange.csv", "csv_test/longOutOfRange.txt");
    longOverflow.open();

    for (int i = 0; i < 8; i++) {
        ASSERT_THROWS_CODE(longOverflow.read(buf, bufSize), DBException, ErrorCodes::Overflow);
    }
    longOverflow.close();
}

TEST_F(CsvFileInputTest, BufferTooSmall) {
    // Read should throw exception when buffer size is too small.
    CsvFileInput input("csv_test/bufTooSmall.csv", "csv_test/bufTooSmall.txt");
    input.open();

    int bufSize = 5;
    char buf[bufSize];

    for (int i = 0; i < 4; i++) {
        ASSERT_THROWS_CODE(input.read(buf, bufSize), DBException, 200000402);
    }

    input.close();
}

TEST_F(CsvFileInputTest, FailByFileDoesNotExist) {
    CsvFileInput input("DNE.csv", "DNE.txt");
    ASSERT_THROWS_CODE(input.open(), DBException, ErrorCodes::FileNotOpen);

    CsvFileInput input1("DNE1.csv", "DNE1.txt");
    ASSERT_THROWS_CODE(input1.open(), DBException, ErrorCodes::FileNotOpen);

    CsvFileInput input2("DNE2.csv", "csv_test/badOid.txt");
    ASSERT_THROWS_CODE(input2.open(), DBException, ErrorCodes::FileNotOpen);
}

TEST_F(CsvFileInputTest, FailByBadFilePathFormat) {
    ASSERT_THROWS_CODE(
        CsvFileInput("../diffLength.csv", "../csv_test/diffLength.txt"), DBException, 200000400);

    ASSERT_THROWS_CODE(CsvFileInput("../DNE1.csv", "../DNE1.txt"), DBException, 200000400);

    ASSERT_THROWS_CODE(
        CsvFileInput("basicRead.csv",
                     "../Users/youngjoonkim/mongo/src/mongo/db/storage/csv_test /tmp/"),
        DBException,
        200000401);
}

TEST_F(CsvFileInputTest, FailByBadMetadata) {
    CsvFileInput input("csv_test/badMetadata.csv", "csv_test/badMetadata.txt");
    ASSERT_THROWS_CODE(input.open(), DBException, 200000403);

    CsvFileInput input1("csv_test/badMetadata.csv", "csv_test/badMetadata1.txt");
    ASSERT_THROWS_CODE(input1.open(), DBException, 200000403);

    CsvFileInput input2("csv_test/badMetadata.csv", "csv_test/badMetadata2.txt");
    ASSERT_THROWS_CODE(input2.open(), DBException, 200000404);

    CsvFileInput input3("csv_test/badMetadata.csv", "csv_test/badMetadata3.txt");
    ASSERT_THROWS_CODE(input3.open(), DBException, 200000403);
}

}  // namespace mongo
