package org.opensearch.dataprepper.plugins.source.neptune.converter;

public class MetadataKeyAttributes {

    static final String ID_METADATA_ATTRIBUTE = "id";

    static final String NEPTUNE_COMMIT_TIMESTAMP_METADATA_ATTRIBUTE = "commitTimestamp";

    static final String EVENT_VERSION_FROM_TIMESTAMP = "document_version";

    static final String NEPTUNE_STREAM_OP_NAME_METADATA_ATTRIBUTE = "op";

    static final String EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE = "opensearch_action"; // index or update, etc

    static final String INGESTION_EVENT_TYPE_ATTRIBUTE = "ingestion_type"; // stream or export

    static final String EVENT_S3_PARTITION_KEY = "s3_partition_key";

}

