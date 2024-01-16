/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.record;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * The <b>RecordMetadata</b> class provides a wrapper around the ImmutableMap making metadata management easier for the
 * user to access.
 */
public class RecordMetadata {
    private static final RecordMetadata DEFAULT_METADATA = new RecordMetadata();

    //forcing it to a concrete type to show that I want it to be Immutable
    private final ImmutableMap<String, Object> metadata;

    /** Below are the set of keys we always expect to be present in the metadata object. */
    public static final String RECORD_TYPE = "record_type"; //Key for the type of record, such as OTEL-TRACE or LOG

    /**
     * Create a basic metadata object with a record_type of "unknown".
     */
    private RecordMetadata() {
        metadata = ImmutableMap.of(RECORD_TYPE, "unknown");
    }

    /**
     * Creates the object with a set of attributes added. We will expect "record_type" to be present and non-null.
     * @param attributes the attributes to build the metadata object from
     * @throws RuntimeException if there is no value for record_type.
     */
    private RecordMetadata(final Map<String, Object> attributes) {
        metadata = ImmutableMap.copyOf(attributes);
        if (!metadata.containsKey(RECORD_TYPE)) {
            throw new RuntimeException("RecordMetadata must have a key:value pair for " + RECORD_TYPE);
        }
    }

    /**
     * Retrieve an attribute and cast it into the simple type.
     * @param attributeName the attribute to get
     * @return the attribute as a String.
     */
    public String getAsString(String attributeName) {
        return (String) metadata.get(attributeName);
    }

    /**
     * Returns the raw metadata object.
     * @return the raw metadata
     */
    public ImmutableMap<String, Object> getMetadataObject() {
        return metadata;
    }

    /**
     * Creates an empty MetadataRecords object with a record type of unknown.
     * @return an empty MetadataRecords object.
     */
    public static RecordMetadata defaultMetadata() {
        return DEFAULT_METADATA;
    }

    /**
     * Create a RecordMetadata object with the selected attributes.
     * @param attributes the key:value pair of attributes
     * @return a new RecordMetadata object with the given attributes
     */
    public static RecordMetadata of(final Map<String, Object> attributes) {
        return new RecordMetadata(attributes);
    }
}
