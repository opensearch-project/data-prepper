package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


import org.junit.jupiter.api.BeforeEach;

public class JsonDecoderTest {
    private JsonDecoder jsonDecoder;
    private Record<Event> receivedRecord;
    private Instant receivedTime;

    private JsonDecoder createObjectUnderTest() {
        return new JsonDecoder();
    }

    @BeforeEach
    void setup() {
        jsonDecoder = createObjectUnderTest();
        receivedRecord = null;
    }

    @Test
    void test_basicJsonDecoder() {
        String stringValue = UUID.randomUUID().toString();
        Random r = new Random();
        int intValue = r.nextInt();
        String inputString = "[{\"key1\":\"" + stringValue + "\", \"key2\":" + intValue + "}]";
        try {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), null, (record) -> {
                receivedRecord = record;
            });
        } catch (Exception e) {
        }

        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
    }

    @Test
    void test_basicJsonDecoder_exceedingMaxEventLength_throwsException() {
        String largeString = "x".repeat(200);
        String inputString = "[{\"key1\":\"" + largeString + "\"}]";

        jsonDecoder = new JsonDecoder(null, null, null, 100);

        Exception exception = assertThrows(Exception.class, () -> {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), null, (record) -> {
                receivedRecord = record;
            });
        });

        assertEquals("String value length (200) exceeds the maximum allowed (100, from `StreamReadConstraints.getMaxStringLength()`)", exception.getMessage());
    }

    @Test
    void test_basicJsonDecoder_withMaxEventLength() {
        String validString = "Short string";
        String inputString = "[{\"key1\":\"" + validString + "\"}]";

        jsonDecoder = new JsonDecoder(null, null, null, 100);

        assertDoesNotThrow(() -> {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), null, (record) -> {
                receivedRecord = record;
            });
        });

        assertNotNull(receivedRecord);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(validString));
    }

    @Test
    void test_basicJsonDecoder_withTimeReceived() {
        String stringValue = UUID.randomUUID().toString();
        Random r = new Random();
        int intValue = r.nextInt();
        String inputString = "[{\"key1\":\"" + stringValue + "\", \"key2\":" + intValue + "}]";
        final Instant now = Instant.now();
        try {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), now, (record) -> {
                receivedRecord = record;
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });
        } catch (Exception e) {
        }

        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
        assertThat(receivedTime, equalTo(now));
    }

    @Nested
    class JsonDecoderWithInputConfig {
        private ObjectMapper objectMapper;
        private final List<String> includeKeys = new ArrayList<>();
        private final List<String> includeMetadataKeys = new ArrayList<>();
        private static final int numKeyRecords = 10;
        private static final int numKeyPerRecord = 3;
        private Map<String, Object> jsonObject;
        private final String key_name = "logEvents";
        private final Integer maxEventLength = 20000000;

        @BeforeEach
        void setup() {
            objectMapper = new ObjectMapper();
            for (int i = 0; i < 10; i++) {
                includeKeys.add(UUID.randomUUID().toString());
                includeMetadataKeys.add(UUID.randomUUID().toString());
            }
            jsonObject = generateJsonWithSpecificKeys(includeKeys, includeMetadataKeys, key_name, numKeyRecords, numKeyPerRecord);
        }

        @Test
        void test_basicJsonDecoder_withInputConfig() throws IOException {
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            jsonDecoder = new JsonDecoder(key_name, includeKeys, includeMetadataKeys, maxEventLength);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });

            assertFalse(records.isEmpty());
            assertEquals(numKeyRecords, records.size());

            records.forEach(record -> {
                Map<String, Object> dataMap = record.getData().toMap();
                Map<String, Object> metadataMap = record.getData().getMetadata().getAttributes();

                for (String includeKey : includeKeys) {
                    assertThat(dataMap.get(includeKey), equalTo(jsonObject.get(includeKey)));
                }
                for (String includeMetadataKey : includeMetadataKeys) {
                    assertThat(metadataMap.get(includeMetadataKey), equalTo(jsonObject.get(includeMetadataKey)));
                }
            });

            assertThat(receivedTime, equalTo(now));
        }

        @Test
        void test_basicJsonDecoder_withInputConfig_withoutEvents_empty_metadata_keys() throws IOException {
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            jsonDecoder = new JsonDecoder("", includeKeys, Collections.emptyList(), maxEventLength);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });
            assertTrue(records.isEmpty());
        }

        @Test
        void test_basicJsonDecoder_withInputConfig_withoutEvents_null_include_metadata_keys() throws IOException {
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            jsonDecoder = new JsonDecoder("", includeKeys, null, maxEventLength);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });

            assertTrue(records.isEmpty());
        }

        @Test
        void test_basicJsonDecoder_withInputConfig_withoutEvents_empty_include_keys() throws IOException {
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            jsonDecoder = new JsonDecoder("", Collections.emptyList(), includeMetadataKeys, maxEventLength);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });
            assertTrue(records.isEmpty());
        }

        @Test
        void test_basicJsonDecoder_withInputConfig_withoutEvents_null_include_keys() throws IOException {
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            jsonDecoder = new JsonDecoder("", null, includeMetadataKeys, maxEventLength);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });

            assertTrue(records.isEmpty());
        }

        private Map<String, Object> generateJsonWithSpecificKeys(final List<String> includeKeys,
                                                                 final List<String> includeMetadataKeys,
                                                                 final String key,
                                                                 final int numKeyRecords,
                                                                 final int numKeyPerRecord) {
            final Map<String, Object> jsonObject = new LinkedHashMap<>();
            final List<Map<String, Object>> innerObjects = new ArrayList<>();

            for (String includeKey : includeKeys) {
                jsonObject.put(includeKey, UUID.randomUUID().toString());
            }

            for (String includeMetadataKey : includeMetadataKeys) {
                jsonObject.put(includeMetadataKey, UUID.randomUUID().toString());
            }

            for (int i = 0; i < numKeyRecords; i++) {
                final Map<String, Object> innerJsonMap = new LinkedHashMap<>();
                for (int j = 0; j < numKeyPerRecord; j++) {
                    innerJsonMap.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                }
                innerObjects.add(innerJsonMap);
            }
            jsonObject.put(key, innerObjects);
            return jsonObject;
        }

        private InputStream createInputStream(final Map<String, ?> jsonRoot) throws JsonProcessingException {
            final byte[] jsonBytes = objectMapper.writeValueAsBytes(jsonRoot);

            return new ByteArrayInputStream(jsonBytes);
        }
    }

}
