/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

public class MetadataKeyAttributes {
    public static final String PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE = "primary_key";

    public static final String EVENT_VERSION_FROM_TIMESTAMP = "document_version";

    public static final String EVENT_TIMESTAMP_METADATA_ATTRIBUTE = "event_timestamp";

    public static final String BULK_ACTION_METADATA_ATTRIBUTE = "opensearch_action";

    public static final String CHANGE_EVENT_TYPE_METADATA_ATTRIBUTE = "change_event_type";

    public static final String EVENT_DATABASE_NAME_METADATA_ATTRIBUTE = "database_name";

    public static final String EVENT_TABLE_NAME_METADATA_ATTRIBUTE = "table_name";

    public static final String INGESTION_EVENT_TYPE_ATTRIBUTE = "ingestion_type";

    public static final String EVENT_S3_PARTITION_KEY = "s3_partition_key";
}
