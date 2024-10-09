/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.converter;

public class MetadataKeyAttributes {
    public static final String KINESIS_STREAM_NAME_METADATA_ATTRIBUTE = "stream_name";
    public static final String KINESIS_PARTITION_KEY_METADATA_ATTRIBUTE = "partition_key";
    public static final String KINESIS_SEQUENCE_NUMBER_METADATA_ATTRIBUTE = "sequence_number";
}
