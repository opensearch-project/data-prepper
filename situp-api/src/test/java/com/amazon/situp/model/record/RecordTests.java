package com.amazon.situp.model.record;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazon.situp.model.record.RecordMetadata.RECORD_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RecordTests {
    private static final String TEST_DATA = "TEST";
    private static final String TEST_RECORD_TYPE = "OTEL";
    private static final String TEST_RECORD_METADATA_V_TYPE = "VERSION";
    private static final String TEST_RECORD_VERSION = "1.0";

    @Test
    public void testRecordOperations() {
        final Record<String> stringRecord = new Record<>(TEST_DATA);
        final Record<String> stringRecordWithDefaultMetadata = new Record<>(TEST_DATA, RecordMetadata.defaultMetadata());
        assertThat("Incorrect Record data is returned", stringRecord.getData(), is(equalTo(TEST_DATA)));
        assertThat("Incorrect Record data is returned", stringRecordWithDefaultMetadata.getData(),
                is(equalTo(TEST_DATA)));
        final RecordMetadata defaultRecordMetadataWithoutPassing = stringRecord.getMetadata();
        final RecordMetadata defaultRecordMetadataWithPassing = stringRecordWithDefaultMetadata.getMetadata();
        assertThat("Default metadata should match",
                defaultRecordMetadataWithoutPassing.getMetadataObject(),
                is(equalTo(defaultRecordMetadataWithPassing.getMetadataObject())));
        assertThat("Incorrect default metadata for record",
                defaultRecordMetadataWithoutPassing.getAsString(RECORD_TYPE), is(equalTo("unknown")));
        final Map<String, Object> metadataObject = defaultRecordMetadataWithoutPassing.getMetadataObject();
        assertThat("Incorrect record metadata object values", metadataObject.get(RECORD_TYPE),
                is(equalTo("unknown")));
    }

    @Test
    public void testRecordCreationWithMetadata() {
        final Map<String, Object> metadataObjectMap = new HashMap<>();
        metadataObjectMap.put(RECORD_TYPE, TEST_RECORD_TYPE);
        metadataObjectMap.put(TEST_RECORD_METADATA_V_TYPE, TEST_RECORD_VERSION);
        final RecordMetadata recordMetadata = RecordMetadata.of(metadataObjectMap);
        final Record<String> stringRecord = new Record<>(TEST_DATA, recordMetadata);
        assertThat("Incorrect Record data is returned", stringRecord.getData(), is(equalTo(TEST_DATA)));
        final RecordMetadata actualRecordMetadata = stringRecord.getMetadata();
        assertThat("Incorrect record_type metadata for record", actualRecordMetadata.getAsString(RECORD_TYPE),
                is(equalTo(TEST_RECORD_TYPE)));
        assertThat("Incorrect version metadata for record", actualRecordMetadata.getAsString(TEST_RECORD_METADATA_V_TYPE),
                is(equalTo(TEST_RECORD_VERSION)));
        final Map<String, Object> actualMetadataObjectMap = actualRecordMetadata.getMetadataObject();
        assertThat("Incorrect record metadata object map", actualMetadataObjectMap,
                is(equalTo(metadataObjectMap)));
    }

    @Test(expected = RuntimeException.class)
    public void testRecordMetadataWithoutRecordType() {
        final RecordMetadata recordMetadata = RecordMetadata.of(new HashMap<>());
    }
}
