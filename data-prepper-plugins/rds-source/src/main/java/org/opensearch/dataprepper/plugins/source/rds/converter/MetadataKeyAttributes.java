/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

public class MetadataKeyAttributes {
    static final String PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE = "primary_key";

    static final String EVENT_VERSION_FROM_TIMESTAMP = "document_version";

    static final String EVENT_TIMESTAMP_METADATA_ATTRIBUTE = "event_timestamp";

    static final String EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE = "opensearch_action";

    static final String CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE = "change_event_type";

    static final String EVENT_DATABASE_NAME_METADATA_ATTRIBUTE = "database_name";

    static final String EVENT_TABLE_NAME_METADATA_ATTRIBUTE = "table_name";

    static final String INGESTION_EVENT_TYPE_ATTRIBUTE = "ingestion_type";

    static final String EVENT_S3_PARTITION_KEY = "s3_partition_key";
}
