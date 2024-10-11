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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        String inputString = "[{\"key1\":\""+stringValue+"\", \"key2\":"+intValue+"}]";
        try {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), null, (record) -> {
                receivedRecord = record;
            });
        } catch (Exception e){}
        
        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
    }

    @Test
    void test_basicJsonDecoder_withTimeReceived() {
        String stringValue = UUID.randomUUID().toString();
        Random r = new Random();
        int intValue = r.nextInt();
        String inputString = "[{\"key1\":\""+stringValue+"\", \"key2\":"+intValue+"}]";
        final Instant now = Instant.now();
        try {
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), now, (record) -> {
                receivedRecord = record;
                receivedTime = record.getData().getEventHandle().getInternalOriginationTime();
            });
        } catch (Exception e){}

        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
        assertThat(receivedTime, equalTo(now));
    }

    @Nested
    class JsonDecoderWithInputConfig {
        private ObjectMapper objectMapper;
        final List<String> include_keys = new ArrayList<>();

        @BeforeEach
        void setup() {
            objectMapper = new ObjectMapper();
        }
        @Test
        void test_basicJsonDecoder_withInputConfig() throws IOException {
            Random r = new Random();
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            for (int i=0; i<10; i++) {
                include_keys.add(UUID.randomUUID().toString());
            }
            final String key_name = "logEvents";
            Map<String, Object> jsonObject = generateJsonWithSpecificKeys(include_keys, key_name, 10);
            jsonDecoder = new JsonDecoder(key_name, include_keys, include_keys);
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = ((Event)record.getData()).getEventHandle().getInternalOriginationTime();
            });

            records.forEach(record -> {
                Map<String, Object> dataMap = record.getData().toMap();
                Map<String, Object> metadataMap = record.getData().getMetadata().getAttributes();

                for (String include_key: include_keys) {
                    assertThat(dataMap.get(include_key), equalTo(jsonObject.get(include_key)));
                    assertThat(metadataMap.get(include_key), equalTo(jsonObject.get(include_key)));
                }
            });

            assertThat(receivedTime, equalTo(now));
        }

        @Test
        void test_basicJsonDecoder_withInputConfig_withoutEvents() throws IOException {
            Random r = new Random();
            final Instant now = Instant.now();
            List<Record<Event>> records = new ArrayList<>();
            Map<String, Object> jsonObject = generateJsonWithSpecificKeys(include_keys, "logEvents", 10);
            jsonDecoder = new JsonDecoder("", include_keys, Collections.emptyList());
            jsonDecoder.parse(createInputStream(jsonObject), now, (record) -> {
                records.add(record);
                receivedTime = ((Event)record.getData()).getEventHandle().getInternalOriginationTime();
            });

            assertTrue(records.isEmpty());
        }

        private Map<String, Object> generateJsonWithSpecificKeys(final List<String> outerKeys, final String key, final int numRecords) {
            final Map<String, Object> jsonObject = new LinkedHashMap<>();
            final List<Map<String, Object>> innerObjects = new ArrayList<>();

            for (String outerKey: outerKeys) {
                jsonObject.put(outerKey, UUID.randomUUID().toString());
            }
            for (int i=0; i<numRecords; i++) {
                final Map<String, Object> innerJsonMap = new LinkedHashMap<>();
                for (int j=0; j<3; j++) {
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
