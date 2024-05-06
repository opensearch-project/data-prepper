/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

public class MetadataKeyAttributes {
    static final String PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE = "primary_key";
    public static final String DOCUMENTDB_PRIMARY_KEY_ATTRIBUTE_NAME = "_id";

    static final String PARTITION_KEY_METADATA_ATTRIBUTE = "partition_key";
    static final String DOCUMENTDB_ID_TYPE_METADATA_ATTRIBUTE = "documentdb_id_bson_type";

    static final String DOCUMENTDB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE = "documentdb_timestamp";

    static final String EVENT_VERSION_FROM_TIMESTAMP = "document_version";

    static final String EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE = "opensearch_action";

    static final String DOCUMENTDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE = "documentdb_event_name";

    static final String DOCUMENTDB_EVENT_COLLECTION_METADATA_ATTRIBUTE = "documentdb_collection";

    static final String INGESTION_EVENT_TYPE_ATTRIBUTE = "ingestion_type";

    static final String EVENT_S3_PARTITION_KEY = "s3_partition_key";
}

