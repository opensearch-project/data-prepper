package org.opensearch.dataprepper.model.codec;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.BeforeEach;

public class JsonObjDecoderTest {
    private JsonObjDecoder jsonObjDecoder;
    private Record<Event> receivedRecord;
    private Instant receivedTime;

    private JsonObjDecoder createObjectUnderTest() {
        return new JsonObjDecoder();
    }

    @BeforeEach
    void setup() {
        jsonObjDecoder = createObjectUnderTest();
        receivedRecord = null;
    }

    @Test
    void test_basicJsonObjDecoder() {
        String stringValue = UUID.randomUUID().toString();
        Random r = new Random();
        int intValue = r.nextInt();
        String inputString = "{\"key1\":\""+stringValue+"\", \"key2\":"+intValue+"}";
        try {
            jsonObjDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), null, (record) -> {
                receivedRecord = record;
            });
        } catch (Exception e){}

        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
    }

    @Test
    void test_basicJsonObjDecoder_withTimeReceived() {
        String stringValue = UUID.randomUUID().toString();
        Random r = new Random();
        int intValue = r.nextInt();

        String inputString = "{\"key1\":\""+stringValue+"\", \"key2\":"+intValue+"}";
        final Instant now = Instant.now();
        try {
            jsonObjDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), now, (record) -> {
                receivedRecord = record;
                receivedTime = ((DefaultEventHandle)(((Event)record.getData()).getEventHandle())).getInternalOriginationTime();
            });
        } catch (Exception e){}
        
        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
        assertThat(receivedTime, equalTo(now));
    }

}
