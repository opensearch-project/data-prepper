/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.record;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.model.record.RecordMetadata.RECORD_TYPE;

public class RecordTests {
    private static final String TEST_DATA = "TEST";
    private static final String TEST_RECORD_TYPE = "OTEL";
    private static final String TEST_RECORD_METADATA_V_TYPE = "VERSION";
    private static final String UNKNOWN = "unknown";
    private static final String ATTRIBUTE_NOT_PRESENT = "NOT_PRESENT";
    private static final String TEST_RECORD_VERSION = "1.0";

    @Test
    public void testRecordOperations() {
        final Record<String> stringRecord = new Record<>(TEST_DATA);
        assertThat(stringRecord.getData(), is(equalTo(TEST_DATA)));
        final RecordMetadata recordMetadata = stringRecord.getMetadata();
        assertThat(recordMetadata.getAsString(RECORD_TYPE), is(equalTo(UNKNOWN)));
        final Map<String, Object> metadataObject = recordMetadata.getMetadataObject();
        assertThat(metadataObject.get(RECORD_TYPE), is(equalTo(UNKNOWN)));
        assertThat(recordMetadata.getAsString(ATTRIBUTE_NOT_PRESENT), nullValue());
    }

    @Test
    public void testRecordWithMetadata() {
        final Record<String> stringRecord = new Record<>(TEST_DATA, RecordMetadata.defaultMetadata());
        assertThat(stringRecord.getData(), is(equalTo(TEST_DATA)));
        final RecordMetadata recordMetadata = stringRecord.getMetadata();
        final Map<String, Object> metadata = recordMetadata.getMetadataObject();
        assertThat(recordMetadata.getAsString(RECORD_TYPE), is(equalTo(UNKNOWN)));
        assertThat(recordMetadata.getAsString(RECORD_TYPE), is(equalTo(metadata.get(RECORD_TYPE))));
        assertThat(recordMetadata.getAsString(ATTRIBUTE_NOT_PRESENT), nullValue());
    }

    @Test
    public void testRecordUsingDefaultMetadataAndNoMetadata() {
        final Record<String> recordWithMetadata = new Record<>(TEST_DATA, RecordMetadata.defaultMetadata());
        final Record<String> recordWithDefaultMetadata = new Record<>(TEST_DATA);
        assertThat(recordWithMetadata.getData(), is(equalTo(recordWithDefaultMetadata.getData())));
        final RecordMetadata recordMetadata = recordWithMetadata.getMetadata();
        final RecordMetadata defaultMetadata = recordWithDefaultMetadata.getMetadata();
        assertThat(recordMetadata.getMetadataObject(), is(equalTo(defaultMetadata.getMetadataObject())));
        assertThat(recordMetadata.getAsString(RECORD_TYPE), is(equalTo(defaultMetadata.getAsString(RECORD_TYPE))));
    }

    @Test
    public void testRecordCreationWithMetadata() {
        final Map<String, Object> metadataObjectMap = new HashMap<>();
        metadataObjectMap.put(RECORD_TYPE, TEST_RECORD_TYPE);
        metadataObjectMap.put(TEST_RECORD_METADATA_V_TYPE, TEST_RECORD_VERSION);
        final RecordMetadata recordMetadata = RecordMetadata.of(metadataObjectMap);
        final Record<String> stringRecord = new Record<>(TEST_DATA, recordMetadata);
        assertThat(stringRecord.getData(), is(equalTo(TEST_DATA)));
        final RecordMetadata actualRecordMetadata = stringRecord.getMetadata();
        assertThat(actualRecordMetadata.getAsString(RECORD_TYPE), is(equalTo(TEST_RECORD_TYPE)));
        assertThat(actualRecordMetadata.getAsString(TEST_RECORD_METADATA_V_TYPE), is(equalTo(TEST_RECORD_VERSION)));
        final Map<String, Object> actualMetadataObjectMap = actualRecordMetadata.getMetadataObject();
        assertThat(actualMetadataObjectMap, is(equalTo(metadataObjectMap)));
    }

    @Test(expected = RuntimeException.class)
    public void testRecordMetadataWithoutRecordType() {
        final RecordMetadata recordMetadata = RecordMetadata.of(new HashMap<>());
    }
}