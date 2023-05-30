package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GenericRecordJsonEncoderTest {

    private static Schema SCHEMA = new Schema.Parser().parse(
            "{\"namespace\": \"org.example.test\"," +
                    " \"type\": \"record\"," +
                    " \"name\": \"TestMessage\"," +
                    " \"fields\": [" +
                    "     {\"name\": \"nested\", \"type\": \"TestMessage\"}, "+
                    "     {\"name\": \"id\", \"type\": \"string\"}," +
                    "     {\"name\": \"value\", \"type\": \"int\"}," +
                    "     {\"name\": \"floatValue\", \"type\": \"float\"}," +
                    "     {\"name\": \"alternateIds\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}," +
                    "     {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}," +
                    "     {\"name\": \"lastUpdated\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}," +
                    "     {\"name\": \"rawData\", \"type\": \"bytes\"}," +
                    "     {\"name\": \"suit\", \"type\": {\"type\": \"enum\", \"name\": \"Suit\", " +
                    "                \"symbols\" : [\"SPADES\", \"HEARTS\", \"DIAMONDS\", \"CLUBS\"]}}" +
                    " ]}");

    private GenericRecordJsonEncoder encoder;

    @BeforeEach
    void setUp() {
        encoder = new GenericRecordJsonEncoder();
    }

    @Test
    void serialize_WithEmptyRecord_ReturnsEmptyJson() {
        // Test for serializing an empty record
        GenericRecord record = new GenericData.Record(SCHEMA);
        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithNestedRecord_ReturnsCorrectJson() {
        // Test for serializing a nested record
        Schema schema = new Schema.Parser().parse(
                "{" +
                        "  \"type\": \"record\"," +
                        "  \"name\": \"ParentRecord\"," +
                        "  \"fields\": [" +
                        "    {" +
                        "      \"name\": \"child\"," +
                        "      \"type\": {" +
                        "        \"type\": \"record\"," +
                        "        \"name\": \"ChildRecord\"," +
                        "        \"fields\": [" +
                        "          {\"name\": \"name\", \"type\": \"string\"}" +
                        "        ]" +
                        "      }" +
                        "    }" +
                        "  ]" +
                        "}"
        );
        GenericRecord childRecord = new GenericData.Record(schema.getField("child").schema());
        childRecord.put("name", "John Doe");
        GenericRecord parentRecord = new GenericData.Record(schema);
        parentRecord.put("child", childRecord);

        String expectedJson = "{\"child\": {\"name\": \"John Doe\"}}";

        String json = encoder.serialize(parentRecord);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithCircularReference_ReturnsErrorMessage() {
        // Test for circular reference handling
        GenericRecord record1 = new GenericData.Record(SCHEMA);
        record1.put("nested", record1);

        String expectedErrorMessage = "{\"nested\":  \">>> CIRCULAR REFERENCE CANNOT BE PUT IN JSON STRING, ABORTING RECURSION <<<\" , \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record1);

        assertEquals(expectedErrorMessage, json);
    }

    @Test
    void serialize_WithArray_ReturnsCorrectJson() {
        // Test for serializing an array
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("alternateIds", new GenericData.Array<>(SCHEMA.getField("alternateIds").schema(), java.util.Arrays.asList("one", "two", "three")));

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": [\"one\", \"two\", \"three\"], \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithMap_ReturnsCorrectJson() {
        // Test for serializing a map
        GenericRecord record = new GenericData.Record(SCHEMA);
        Map<String, String> map = new HashMap<>();
        map.put("one", "valueOne");
        map.put("two", "valueTwo");
        map.put("three", "valueThree");
        record.put("metadata", map);

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": {\"one\": \"valueOne\", \"two\": \"valueTwo\", \"three\": \"valueThree\"}, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithEnum_ReturnsCorrectJson() {
        // Test for serializing an enum
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("suit", new GenericData.EnumSymbol(SCHEMA.getField("suit").schema(), "SPADES"));

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": \"SPADES\"}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithNullValue_ReturnsCorrectJson() {
        // Test for serializing null value
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("nested", null);

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithBytesValue_ReturnsCorrectJson() {
        // Test for serializing bytes value
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("rawData", ByteBuffer.wrap(new byte[]{1, 2, 3}));

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": {\"bytes\": \"\\u0001\\u0002\\u0003\"}, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithFloatNaNValue_ReturnsQuotedJson() {
        // Test for serializing float NaN value
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("floatValue", Float.NaN);

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": \"NaN\", \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithFloatInfinityValue_ReturnsQuotedJson() {
        // Test for serializing float Infinity value
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("floatValue", Float.POSITIVE_INFINITY);

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": \"Infinity\", \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void serialize_WithBytesContainingSpecialCharacters_ReturnsEscapedJson() {
        // Test for serializing bytes with special characters
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("rawData", ByteBuffer.wrap(new byte[]{34, 92, 13, 10, 9}));

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": null, \"rawData\": {\"bytes\": \"\\\"\\\\\\r\\n\\t\"}, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

    @Test
    void registerLogicalTypeConverter_WithLogicalType_ConvertsValueUsingConverter() {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("lastUpdated", Instant.ofEpochMilli(1685483879));

        String expectedJson = "{\"nested\": null, \"id\": null, \"value\": null, \"floatValue\": null, \"alternateIds\": null, \"metadata\": null, \"lastUpdated\": 1970-01-20T12:11:23.879Z, \"rawData\": null, \"suit\": null}";

        String json = encoder.serialize(record);

        assertEquals(expectedJson, json);
    }

}