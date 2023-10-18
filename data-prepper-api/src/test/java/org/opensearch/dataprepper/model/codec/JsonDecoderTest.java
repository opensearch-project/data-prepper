package org.opensearch.dataprepper.model.codec;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;

import org.junit.jupiter.api.BeforeEach;

public class JsonDecoderTest {
    private JsonDecoder jsonDecoder;
    private Record<Event> receivedRecord;

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
            jsonDecoder.parse(new ByteArrayInputStream(inputString.getBytes()), (record) -> {
                receivedRecord = record;
            });
        } catch (Exception e){}
        
        assertNotEquals(receivedRecord, null);
        Map<String, Object> map = receivedRecord.getData().toMap();
        assertThat(map.get("key1"), equalTo(stringValue));
        assertThat(map.get("key2"), equalTo(intValue));
    }

}
