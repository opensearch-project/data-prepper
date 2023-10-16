/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

public class MetadataKeyAttributes {
     static final String PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE = "primary_key";

     static final String PARTITION_KEY_METADATA_ATTRIBUTE = "partition_key";

     static final String SORT_KEY_METADATA_ATTRIBUTE = "sort_key";

     static final String EVENT_TIMESTAMP_METADATA_ATTRIBUTE = "ts";

     static final String STREAM_EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE = "op";

     static final String EVENT_TABLE_NAME_METADATA_ATTRIBUTE = "table_name";
}
