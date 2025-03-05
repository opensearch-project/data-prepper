package org.opensearch.dataprepper.plugins.mongo.client;

import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.JSON_WRITER_SETTINGS;
import static org.opensearch.dataprepper.plugins.mongo.client.BsonHelper.PARTITION_FORMAT;

@ExtendWith(MockitoExtension.class)
public class BsonHelperTest {
    private final Random random = new Random();

    @ParameterizedTest
    @MethodSource("mongoDataTypeProvider")
    public void getDocumentPartitionStringFromMongoDBId(final String actualDocument, final String expectedDocument) {
        final Document document = Document.parse(actualDocument);
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Document.class.getName());
        assertThat(partition, is(expectedDocument));
    }

    @Test
    public void getBinaryPartitionStringFromMongoDBId() {
        final Binary document = mock(Binary.class);
        final byte type =  getRandomByte();
        final byte[] byteData =  new byte[] { getRandomByte() };
        when(document.getType()).thenReturn(type);
        when(document.getData()).thenReturn(byteData);
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Binary.class.getName());
        assertThat(partition, is(String.format("%s-%s", type, Base64.getEncoder().encodeToString(byteData))));
    }

    @Test
    public void getBsonDateTimePartitionStringFromMongoDBId() {
        final BsonDateTime document = mock(BsonDateTime.class);
        when(document.getValue()).thenReturn(Long.valueOf(getRandomInteger()));
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, BsonDateTime.class.getName());
        assertThat(partition, is(String.valueOf(document.getValue())));
    }

    @Test
    public void getBsonTimestampPartitionStringFromMongoDBId() {
        final BsonTimestamp document = mock(BsonTimestamp.class);
        when(document.getInc()).thenReturn(getRandomInteger());
        when(document.getTime()).thenReturn(getRandomInteger());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, BsonTimestamp.class.getName());
        assertThat(partition, is(String.format("%s-%s", document.getTime(), document.getInc())));
    }

    @Test
    public void getCodePartitionStringFromMongoDBId() {
        final Code document = mock(Code.class);
        when(document.getCode()).thenReturn(UUID.randomUUID().toString());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Code.class.getName());
        assertThat(partition, is(document.getCode()));
    }

    @Test
    public void getDecimal128PartitionStringFromMongoDBId() {
        final Decimal128 document = mock(Decimal128.class);
        final BigDecimal bigDecimal = BigDecimal.valueOf(new Random().nextDouble());
        when(document.bigDecimalValue()).thenReturn(bigDecimal);
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Decimal128.class.getName());
        assertThat(partition, is(bigDecimal.toPlainString()));
    }

    @Test
    public void getPartitionStringFromMongoDBId() {
        final ObjectId document = mock(ObjectId.class);
        when(document.toString()).thenReturn(UUID.randomUUID().toString());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, ObjectId.class.getName());
        assertThat(partition, is(document.toString()));
    }

    @Test
    public void buildAndQueryForIntegerClass() {
        final String gteValue = String.valueOf(getRandomInteger());
        final String lteValue = String.valueOf(getRandomInteger());
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Integer.class.getName(), Integer.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForLongClass() {
        final String gteValue = String.valueOf(Long.valueOf(getRandomInteger()));
        final String lteValue = String.valueOf(Long.valueOf(getRandomInteger()));
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Long.class.getName(), Long.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDoubleClass() {
        final String gteValue = String.valueOf(Double.valueOf(getRandomInteger()));
        final String lteValue = String.valueOf(Double.valueOf(getRandomInteger()));
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Double.class.getName(), Double.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForStringClass() {
        final String gteValue = UUID.randomUUID().toString();
        final String lteValue = UUID.randomUUID().toString();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": \"%s\"}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": \"%s\"}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, String.class.getName(), String.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForObjectIdClass() {
        final String gteValue = getRandomHexStringLength24();
        final String lteValue = getRandomHexStringLength24();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$oid\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$oid\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, ObjectId.class.getName(), ObjectId.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDecimal128Class() {
        final String gteValue = (new Decimal128(getRandomInteger())).toString();
        final String lteValue = (new Decimal128(getRandomInteger())).toString();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$numberDecimal\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$numberDecimal\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Decimal128.class.getName(), Decimal128.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForCodeClass() {
        final String gteValue = (new Code(UUID.randomUUID().toString())).getCode();
        final String lteValue = (new Code(UUID.randomUUID().toString())).getCode();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$code\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$code\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Code.class.getName(), Code.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForSymbolClass() {
        final String gteValue = (new Symbol(UUID.randomUUID().toString())).getSymbol();
        final String lteValue = (new Symbol(UUID.randomUUID().toString())).getSymbol();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$symbol\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$symbol\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Symbol.class.getName(), Symbol.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDocumentClass() {
        final String gteValue = Document.parse(String.format("{\"%s\":\"%s\"}",  UUID.randomUUID(),  UUID.randomUUID())).toJson();
        final String lteValue = Document.parse(String.format("{\"%s\":\"%s\"}",  UUID.randomUUID(),  UUID.randomUUID())).toJson();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Document.class.getName(), Document.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForBsonDateTimeClass() {
        final String gteValue = String.valueOf(Math.abs(new Random().nextLong()));
        final String lteValue = String.valueOf(Math.abs(new Random().nextLong()));
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$date\": {\"$numberLong\": \"%s\"}}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$date\": {\"$numberLong\": \"%s\"}}}}", lteValue);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, BsonDateTime.class.getName(), BsonDateTime.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForBsonTimestampClass() {
        int tValue1 = Math.abs(new Random().nextInt());
        int iValue1 = Math.abs(new Random().nextInt());
        int tValue2 = Math.abs(new Random().nextInt());
        int iValue2 = Math.abs(new Random().nextInt());
        final String gteValue = String.format(PARTITION_FORMAT, tValue1, iValue1);
        final String lteValue = String.format(PARTITION_FORMAT, tValue2, iValue2);
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$timestamp\": {\"t\": %s, \"i\": %s}}}}", tValue1, iValue1);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$timestamp\": {\"t\": %s, \"i\": %s}}}}", tValue2, iValue2);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, BsonTimestamp.class.getName(), BsonTimestamp.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForBinaryClass() {
        final String bytes1String = UUID.randomUUID().toString();
        final String bytes1 = Base64.getEncoder().encodeToString(bytes1String.getBytes());
        int value1 = new Random().nextInt(10);
        final String bytes1Type = String.format("%02x",(Math.abs(value1)));
        int value2 = new Random().nextInt(10);
        final String bytes2String = UUID.randomUUID().toString();
        final String bytes2 = Base64.getEncoder().encodeToString(bytes2String.getBytes());
        final String bytes2Type = String.format("%02x",(Math.abs(value2)));
        final String gteValue = String.format(PARTITION_FORMAT, bytes1Type, bytes1);
        final String lteValue = String.format(PARTITION_FORMAT, bytes2Type, bytes2);
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}}", bytes1, bytes1Type);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}}", bytes2, bytes2Type);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Binary.class.getName(), Binary.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForBinaryUUIDClass() {
        final UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        final String base64String = Base64.getEncoder().encodeToString(bb.array());
        final Document document = Document.parse(String.format("{\"_id\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}", base64String, "04"));
        final String record = document.toJson(JSON_WRITER_SETTINGS);
        assertThat(record, is(String.format("{\"_id\": \"%s\"}", uuid)));
    }

    @Test
    public void buildAndQueryForBinaryNonUUIDClass() {
        final String base64String = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        int value = new Random().nextInt(10);
        if (value == 4) value++;
        final String bytesType = String.format("%02x",(Math.abs(value)));
        final Document document = Document.parse(String.format("{\"_id\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}", base64String, bytesType));
        final String record = document.toJson(JSON_WRITER_SETTINGS);
        assertThat(record, is(String.format("{\"_id\": \"%s\"}", base64String)));
    }

    @Test
    public void buildAndQueryForIntegerAndBinaryClass() {
        final String gteValue = String.valueOf(Math.abs(new Random(10_000).nextInt()));
        final String uuid = UUID.randomUUID().toString();
        final String bytesString = Base64.getEncoder().encodeToString(uuid.getBytes());
        int value = new Random().nextInt(10);
        final String bytesType = String.format("%02x",(Math.abs(value)));
        final String lteValue = String.format(PARTITION_FORMAT, bytesType, bytesString);
        final String expectedValueString = String.format("{\"$or\": [{\"$or\": [" +
                "{\"$or\": [{\"_id\": {\"$gte\": %s}}, " +
                "{\"_id\": {\"$gte\": \"\"}}]}, " +
                "{\"_id\": {\"$gte\": {}}}]}, " +
                "{\"_id\": {\"$lte\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}}" +
                "]}",
                gteValue, bytesString, bytesType);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Integer.class.getName(), Binary.class.getName());
        assertThat(bson.toBsonDocument().toJson(), is(expectedValueString));
    }

    @Test
    public void buildAndQueryForIntegerAndBsonTimestampClass() {
        final String gteValue = String.valueOf(Math.abs(new Random(10_000).nextInt()));
        int tValue1 = Math.abs(new Random().nextInt());
        int iValue1 = Math.abs(new Random().nextInt());
        final String lteValue = String.format(PARTITION_FORMAT, tValue1, iValue1);
        final String expectedValueString = String.format("{\"$or\": [{\"$or\": [{\"$or\": [{\"$or\": [{\"$or\": [{\"$or\": [{\"$or\": [{\"$or\": [" +
                        "{\"_id\": {\"$gte\": %s}}, " +
                        "{\"_id\": {\"$gte\": \"\"}}]}, " +
                        "{\"_id\": {\"$gte\": {}}}]}, " +
                        "{\"_id\": {\"$gte\": {\"$binary\": {\"base64\": \"\", \"subType\": \"00\"}}}}]}, " +
                        "{\"_id\": {\"$gte\": {\"$oid\": \"000000000000000000000000\"}}}]}, " +
                        "{\"_id\": {\"$in\": [true, false]}}]}, " +
                        "{\"_id\": {\"$gte\": {\"$date\": \"1970-01-01T00:00:00Z\"}}}]}, " +
                        "{\"_id\": {\"$gte\": {\"$date\": \"1970-01-01T00:00:00Z\"}}}]}, " +
                        "{\"_id\": {\"$lte\": {\"$timestamp\": {\"t\": %s, \"i\": %s}}}}]}",
                gteValue, tValue1, iValue1);

        //String.format("{\"_id\": {\"$gte\": {\"$binary\": {\"base64\": \"%s\", \"subType\": \"%s\"}}}}", bytes1, bytes1Type);
        final Bson bson = BsonHelper.buildQuery(gteValue, lteValue, Integer.class.getName(), BsonTimestamp.class.getName());
        assertThat(bson.toBsonDocument().toJson(), is(expectedValueString));
    }

    @Test
    public void buildAndQueryForUnSupportedClass() {
        final String gteValue = new Object().toString();
        final String lteValue = new Object().toString();
        Assertions.assertThrows(RuntimeException.class, () -> BsonHelper.buildQuery(gteValue, lteValue, Class.class.getName(), Class.class.getName()));
    }

    private byte getRandomByte() {
        return (byte)random.nextInt(256);
    }

    private int getRandomInteger() {
        return random.nextInt(10000);
    }

    private String getRandomHexStringLength24(){
        final int numOfChars = 24;
        final Random r = new Random();
        final StringBuilder sb = new StringBuilder();
        while(sb.length() < numOfChars){
            sb.append(Integer.toHexString(r.nextInt()));
        }

        return sb.substring(0, numOfChars);
    }

    private static Stream<Arguments> mongoDataTypeProvider() {
        return Stream.of(
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"name\": \"Hello User\"}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"name\": \"Hello User\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"nullField\": null}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"nullField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"numberField\": 123}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"numberField\": 123}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"doubleValue\": 3.14159}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"doubleValue\": 3.14159}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"longValue\": { \"$numberLong\": \"1234567890123456768\"}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"longValue\": 1234567890123456768}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"stringField\": \"Hello, Mongo!\"}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"stringField\": \"Hello, Mongo!\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"booleanField\": true}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"booleanField\": true}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"dateField\": { \"$date\": \"2024-05-03T13:57:51.155Z\"}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"dateField\": 1714744671155}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"arrayField\": [\"a\",\"b\",\"c\"]}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"arrayField\": [\"a\", \"b\", \"c\"]}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectField\": { \"nestedKey\": \"nestedValue\"}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectField\": {\"nestedKey\": \"nestedValue\"}}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"binaryField\": { \"$binary\": {\"base64\": \"AQIDBA==\", \"subType\": \"00\"}}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"binaryField\": \"AQIDBA==\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"objectIdField\": { \"$oid\": \"6634ed693ac62386d57b12d0\" }}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"objectIdField\": \"6634ed693ac62386d57b12d0\"}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"timestampField\": { \"$timestamp\": {\"t\": 1714744681, \"i\": 29}}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"timestampField\": 1714744681}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"regexField\": { \"$regularExpression\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"regexField\": {\"pattern\": \"^ABC\", \"options\": \"i\"}}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"minKeyField\": { \"$minKey\": 1}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"minKeyField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"maxKeyField\": { \"$maxKey\": 1}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"maxKeyField\": null}"),
                Arguments.of(
                        "{\"_id\": { \"$oid\": \"6634ed693ac62386d57bcaf0\" }, \"bigDecimalField\": { \"$numberDecimal\": \"123456789.0123456789\"}}",
                        "{\"_id\": \"6634ed693ac62386d57bcaf0\", \"bigDecimalField\": \"123456789.0123456789\"}")
        );
    }
}
