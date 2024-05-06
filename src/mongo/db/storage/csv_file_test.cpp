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
        system("cp -r src/mongo/db/storage/csv_test /tmp/");
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

    constexpr int bufSize = 200;
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

    constexpr int bufSize = 250;
    char buf[bufSize];

    for (int i = 0; i < 11; i++) {
        input.read(buf, bufSize);
        ASSERT_BSONOBJ_EQ(BSONObj(buf), expected[i]);
    }

    input.close();
}

TEST_F(CsvFileInputTest, CollectInvalidOID) {
    CsvFileInput invalidOid("csv_test/badOid.csv", "csv_test/badOid.txt");
    invalidOid.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!invalidOid.isEof()) {
        invalidOid.read(buf, bufSize);
    }
    invalidOid.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidOid.releaseIoStats().release())};
    ASSERT_NE(csvFileIoStats, nullptr);
    ASSERT_EQ(csvFileIoStats->_invalidOid, 13);
    ASSERT_EQ(csvFileIoStats->_invalidInt32, 14);
    ASSERT_EQ(csvFileIoStats->_invalidDate, 14);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 41);
}

TEST_F(CsvFileInputTest, CollectInvalidInt32) {
    CsvFileInput invalidInt32("csv_test/badInt.csv", "csv_test/badInt.txt");
    invalidInt32.open();

    constexpr int bufSize = 25;
    char buf[bufSize];

    while (!invalidInt32.isEof()) {
        invalidInt32.read(buf, bufSize);
    }
    invalidInt32.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidInt32.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_invalidInt32, 6);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 6);
}

TEST_F(CsvFileInputTest, CollectInvalidDate) {
    CsvFileInput invalidDate("csv_test/badDate.csv", "csv_test/badDate.txt");
    invalidDate.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!invalidDate.isEof()) {
        invalidDate.read(buf, bufSize);
    }
    invalidDate.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidDate.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_invalidDate, 4);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 4);
}

TEST_F(CsvFileInputTest, CollectInvalidInt64) {
    CsvFileInput invalidInt64("csv_test/badLong.csv", "csv_test/badLong.txt");
    invalidInt64.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!invalidInt64.isEof()) {
        invalidInt64.read(buf, bufSize);
    }
    invalidInt64.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidInt64.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_invalidInt64, 5);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 5);
}

TEST_F(CsvFileInputTest, CollectInvalidBoolean) {
    CsvFileInput invalidBoolean("csv_test/badBoolean.csv", "csv_test/badBoolean.txt");
    invalidBoolean.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!invalidBoolean.isEof()) {
        invalidBoolean.read(buf, bufSize);
    }
    invalidBoolean.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidBoolean.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_invalidBoolean, 11);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 11);
}

TEST_F(CsvFileInputTest, CollectInvalidDouble) {
    CsvFileInput invalidDouble("csv_test/badDecimal.csv", "csv_test/badDecimal.txt");
    invalidDouble.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!invalidDouble.isEof()) {
        invalidDouble.read(buf, bufSize);
    }
    invalidDouble.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(invalidDouble.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_invalidDouble, 4);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 4);
}

TEST_F(CsvFileInputTest, CollectOutOfRange) {
    CsvFileInput int32OutOfRange("csv_test/intOutOfRange.csv", "csv_test/intOutOfRange.txt");
    int32OutOfRange.open();

    constexpr int bufSize = 100;
    char buf[bufSize];

    while (!int32OutOfRange.isEof()) {
        int32OutOfRange.read(buf, bufSize);
    }
    int32OutOfRange.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(int32OutOfRange.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_outOfRange, 6);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 6);

    CsvFileInput int64OutOfRange("csv_test/longOutOfRange.csv", "csv_test/longOutOfRange.txt");
    int64OutOfRange.open();

    while (!int64OutOfRange.isEof()) {
        int64OutOfRange.read(buf, bufSize);
    }
    int64OutOfRange.close();

    std::unique_ptr<CsvFileIoStats> csvFileIoStats2{
        dynamic_cast<CsvFileIoStats*>(int64OutOfRange.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats2->_outOfRange, 8);
    ASSERT_EQ(csvFileIoStats2->_totalErrorCount, 8);
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

TEST_F(CsvFileInputTest, ErrorCount) {
    CsvFileInput input("csv_test/errorCount.csv", "csv_test/errorCount.txt");
    input.open();

    std::vector expected = {

        fromjson(R"(
{
    kString: "string",
    number: null,
    distant: null,
    quadruple: null,
    RightOrWrong: null,
    identifier: null, 
    signOn: null
})"),
        fromjson(R"(
{
    kString: "holyMoly",
    number: 34,
    distant: 1234567890,
    quadruple: 35.23,
    RightOrWrong: true,
    identifier: ObjectId("123456789012345678901234"), 
    signOn: {$date: "2024-04-12T13:36:37.100-06:00"}
})"),
        fromjson(R"(
{
    kString: "Christopher Columbus",
    number: 48,
    distant: 12345678901,
    quadruple: 48.12,
    RightOrWrong: null,
    identifier: null,
    signOn: {$date: "2024-04-11T13:34:34.343Z"}
})"),
        fromjson(R"(
{
    kString: "Backpack",
    number: 55,
    distant: 33,
    quadruple: 45.0,
    RightOrWrong: null,
    identifier: null,
    signOn: null
})"),
        fromjson(R"(
{
    kString: "Cannot",
    number: null,
    distant: null,
    quadruple: 33.4,
    RightOrWrong: true,
    identifier: ObjectId("123456789123456789abcdef"),
    signOn: null
})"),
        fromjson(R"(
{
    kString: "smoking Hot",
    number: 34,
    distant: 12345678901,
    quadruple: null,
    RightOrWrong: null,
    identifier: null,
    signOn: null
})"),
        fromjson(R"(
{
    kString: null,
    number: null,
    distant: null,
    quadruple: 90.09,
    RightOrWrong: false,
    identifier: null,
    signOn: null
})"),
        fromjson(R"(
{
    kString: "Really Really Really Really Really Long String I mean Really Really Long",
    number: 3,
    distant: 45,
    quadruple: 1.2,
    RightOrWrong: false,
    identifier: ObjectId("884cdc3ef43ff10ca56e23fd"),
    signOn: null
})"),
        fromjson(R"(
{
    kString: "Strong and Sound",
    number: 23,
    distant: 9000000000,
    quadruple: 1345.232,
    RightOrWrong: true
})")};

    constexpr int bufSize = 200;
    char buf[bufSize];
    for (const BSONObj& expect : expected) {
        input.read(buf, bufSize);
        ASSERT_BSONOBJ_EQ(BSONObj(buf), expect);
    }

    std::unique_ptr<CsvFileIoStats> csvFileIoStats{
        dynamic_cast<CsvFileIoStats*>(input.releaseIoStats().release())};
    ASSERT_EQ(csvFileIoStats->_incompleteConversionToNumeric, 4);
    ASSERT_EQ(csvFileIoStats->_invalidInt32, 1);
    ASSERT_EQ(csvFileIoStats->_invalidInt64, 1);
    ASSERT_EQ(csvFileIoStats->_invalidDouble, 2);
    ASSERT_EQ(csvFileIoStats->_outOfRange, 4);
    ASSERT_EQ(csvFileIoStats->_invalidDate, 6);
    ASSERT_EQ(csvFileIoStats->_invalidOid, 5);
    ASSERT_EQ(csvFileIoStats->_invalidBoolean, 4);
    ASSERT_EQ(csvFileIoStats->_nonCompliantWithMetadata, 1);
    ASSERT_EQ(csvFileIoStats->_totalErrorCount, 28);

    input.close();
}

TEST_F(CsvFileInputTest, SpecialNumericCase) {
    CsvFileInput input("csv_test/specialNumeric.csv", "csv_test/specialNumeric.txt");
    input.open();

    std::vector expected = {

        fromjson(R"({ stodSpecial: nan })"),
        fromjson(R"({ stodSpecial: nan })"),
        fromjson(R"({ stodSpecial: nan })"),
        fromjson(R"({ stodSpecial: nan })"),
        fromjson(R"({ stodSpecial: INF })"),
        fromjson(R"({ stodSpecial: INF })"),
        fromjson(R"({ stodSpecial: INF })"),
        fromjson(R"({ stodSpecial: INF })"),
        fromjson(R"({ stodSpecial: -INF })"),
        fromjson(R"({ stodSpecial: -INF })"),
        fromjson(R"({ stodSpecial: 4.5123e+10 })"),
        fromjson(R"({ stodSpecial: 6.634 })"),
        fromjson(R"({ stodSpecial: 6711340000000 })"),
        fromjson(R"({ stodSpecial: 9.024434 })"),
        fromjson(R"({ stodSpecial: nan })")};

    constexpr int bufSize = 100;
    char buf[bufSize];

    for (int i = 0; i < 14; i++) {
        input.read(buf, bufSize);
        ASSERT_BSONOBJ_EQ(BSONObj(buf), expected[i]);
    }
}

TEST_F(CsvFileInputTest, ErrorCountOperatorTest) {
    CsvFileIoStats csvFileIoStats1;
    csvFileIoStats1._incompleteConversionToNumeric = 4;
    csvFileIoStats1._invalidInt32 = 1;
    csvFileIoStats1._invalidInt64 = 1;
    csvFileIoStats1._invalidDouble = 2;
    csvFileIoStats1._outOfRange = 4;
    csvFileIoStats1._invalidDate = 6;
    csvFileIoStats1._invalidOid = 5;
    csvFileIoStats1._invalidBoolean = 4;
    csvFileIoStats1._nonCompliantWithMetadata = 1;
    csvFileIoStats1._totalErrorCount = 28;

    CsvFileIoStats csvFileIoStats2;
    csvFileIoStats2._incompleteConversionToNumeric = 1;
    csvFileIoStats2._invalidInt32 = 1;
    csvFileIoStats2._invalidInt64 = 1;
    csvFileIoStats2._invalidDouble = 1;
    csvFileIoStats2._outOfRange = 1;
    csvFileIoStats2._invalidDate = 1;
    csvFileIoStats2._invalidOid = 1;
    csvFileIoStats2._invalidBoolean = 1;
    csvFileIoStats2._nonCompliantWithMetadata = 1;
    csvFileIoStats2._totalErrorCount = 9;

    CsvFileIoStats csvFileIoStats3;
    csvFileIoStats3._incompleteConversionToNumeric = 1;
    csvFileIoStats3._invalidInt32 = 3;
    csvFileIoStats3._invalidInt64 = 2;
    csvFileIoStats3._invalidDouble = 4;
    csvFileIoStats3._outOfRange = 2;
    csvFileIoStats3._invalidDate = 1;
    csvFileIoStats3._invalidOid = 2;
    csvFileIoStats3._invalidBoolean = 4;
    csvFileIoStats3._nonCompliantWithMetadata = 3;
    csvFileIoStats3._totalErrorCount = 22;

    auto totalErrorStats = csvFileIoStats1 + csvFileIoStats2 + csvFileIoStats3;
    ASSERT_EQ(totalErrorStats._incompleteConversionToNumeric, 6);
    ASSERT_EQ(totalErrorStats._invalidInt32, 5);
    ASSERT_EQ(totalErrorStats._invalidInt64, 4);
    ASSERT_EQ(totalErrorStats._invalidDouble, 7);
    ASSERT_EQ(totalErrorStats._outOfRange, 7);
    ASSERT_EQ(totalErrorStats._invalidDate, 8);
    ASSERT_EQ(totalErrorStats._invalidOid, 8);
    ASSERT_EQ(totalErrorStats._invalidBoolean, 9);
    ASSERT_EQ(totalErrorStats._nonCompliantWithMetadata, 5);
    ASSERT_EQ(totalErrorStats._totalErrorCount, 59);

    csvFileIoStats1 += csvFileIoStats2 + csvFileIoStats3;
    ASSERT_EQ(csvFileIoStats1._incompleteConversionToNumeric, 6);
    ASSERT_EQ(csvFileIoStats1._invalidInt32, 5);
    ASSERT_EQ(csvFileIoStats1._invalidInt64, 4);
    ASSERT_EQ(csvFileIoStats1._invalidDouble, 7);
    ASSERT_EQ(csvFileIoStats1._outOfRange, 7);
    ASSERT_EQ(csvFileIoStats1._invalidDate, 8);
    ASSERT_EQ(csvFileIoStats1._invalidOid, 8);
    ASSERT_EQ(csvFileIoStats1._invalidBoolean, 9);
    ASSERT_EQ(csvFileIoStats1._nonCompliantWithMetadata, 5);
    ASSERT_EQ(csvFileIoStats1._totalErrorCount, 59);
    ASSERT_NE(&totalErrorStats, &csvFileIoStats1);
}

}  // namespace mongo
