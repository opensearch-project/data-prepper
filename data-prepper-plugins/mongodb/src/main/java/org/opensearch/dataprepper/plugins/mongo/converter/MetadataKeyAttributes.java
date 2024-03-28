/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

public class MetadataKeyAttributes {
    static final String PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE = "primary_key";
    static final String MONGODB_PRIMARY_KEY_ATTRIBUTE_NAME = "_id";

    static final String PARTITION_KEY_METADATA_ATTRIBUTE = "partition_key";

    static final String MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE = "mongodb_timestamp";

    static final String EVENT_VERSION_FROM_TIMESTAMP = "document_version";

    static final String EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE = "opensearch_action";

    static final String MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE = "mongodb_event_name";

    static final String MONGODB_EVENT_COLLECTION_METADATA_ATTRIBUTE = "mongodb_collection";

    static final String INGESTION_EVENT_TYPE_ATTRIBUTE = "ingestion_type";
}

