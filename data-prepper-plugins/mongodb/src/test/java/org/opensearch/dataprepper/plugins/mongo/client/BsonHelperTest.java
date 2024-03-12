package org.opensearch.dataprepper.plugins.mongo.client;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BsonHelperTest {
    private final Random random = new Random();

    @Test
    public void getDocumentPartitionStringFromMongoDBId() {
        final Document document = mock(Document.class);
        when(document.toJson()).thenReturn(UUID.randomUUID().toString());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Document.class.getName());
        assertThat(partition, is(document.toJson()));
    }

    @Test
    public void getBinaryPartitionStringFromMongoDBId() {
        final Binary document = mock(Binary.class);
        final byte type =  getRandomByte();
        final byte[] byteData =  new byte[] { getRandomByte() };
        when(document.getType()).thenReturn(type);
        when(document.getData()).thenReturn(byteData);
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Binary.class.getName());
        assertThat(partition, is(String.format("%s-%s", type, new String(byteData))));
    }

    @Test
    public void getBSONTimestampPartitionStringFromMongoDBId() {
        final BSONTimestamp document = mock(BSONTimestamp.class);
        when(document.getInc()).thenReturn(getRandomInteger());
        when(document.getTime()).thenReturn(getRandomInteger());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, BSONTimestamp.class.getName());
        assertThat(partition, is(String.format("%s-%s", document.getInc(), document.getTime())));
    }

    @Test
    public void getCodePartitionStringFromMongoDBId() {
        final Code document = mock(Code.class);
        when(document.getCode()).thenReturn(UUID.randomUUID().toString());
        final String partition = BsonHelper.getPartitionStringFromMongoDBId(document, Code.class.getName());
        assertThat(partition, is(document.getCode()));
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
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Integer.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDoubleClass() {
        final String gteValue = String.valueOf(Double.valueOf(getRandomInteger()));
        final String lteValue = String.valueOf(Double.valueOf(getRandomInteger()));
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Double.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForStringClass() {
        final String gteValue = UUID.randomUUID().toString();
        final String lteValue = UUID.randomUUID().toString();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": \"%s\"}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": \"%s\"}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, String.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForLongClass() {
        final String gteValue = String.valueOf(Long.valueOf(getRandomInteger()));
        final String lteValue = String.valueOf(Long.valueOf(getRandomInteger()));
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Long.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForObjectIdClass() {
        final String gteValue = getRandomHexStringLength24();
        final String lteValue = getRandomHexStringLength24();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$oid\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$oid\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, ObjectId.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDecimal128Class() {
        final String gteValue = (new Decimal128(getRandomInteger())).toString();
        final String lteValue = (new Decimal128(getRandomInteger())).toString();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$numberDecimal\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$numberDecimal\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Decimal128.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForCodeClass() {
        final String gteValue = (new Code(UUID.randomUUID().toString())).getCode();
        final String lteValue = (new Code(UUID.randomUUID().toString())).getCode();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$code\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$code\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Code.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForSymbolClass() {
        final String gteValue = (new Symbol(UUID.randomUUID().toString())).getSymbol();
        final String lteValue = (new Symbol(UUID.randomUUID().toString())).getSymbol();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": {\"$symbol\": \"%s\"}}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": {\"$symbol\": \"%s\"}}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Symbol.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForDocumentClass() {
        final String gteValue = Document.parse(String.format("{\"%s\":\"%s\"}",  UUID.randomUUID(),  UUID.randomUUID())).toJson();
        final String lteValue = Document.parse(String.format("{\"%s\":\"%s\"}",  UUID.randomUUID(),  UUID.randomUUID())).toJson();
        final String expectedGteValueString = String.format("{\"_id\": {\"$gte\": %s}}", gteValue);
        final String expectedLteValueString = String.format("{\"_id\": {\"$lte\": %s}}", lteValue);
        final Bson bson = BsonHelper.buildAndQuery(gteValue, lteValue, Document.class.getName());
        assertThat(bson.toBsonDocument().get("$and").asArray().get(0).toString(), is(expectedGteValueString));
        assertThat(bson.toBsonDocument().get("$and").asArray().get(1).toString(), is(expectedLteValueString));
    }

    @Test
    public void buildAndQueryForUnSupportedClass() {
        final String gteValue = new Object().toString();
        final String lteValue =  new Object().toString();
        Assertions.assertThrows(RuntimeException.class, () -> BsonHelper.buildAndQuery(gteValue, lteValue, Class.class.getName()));
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
}
