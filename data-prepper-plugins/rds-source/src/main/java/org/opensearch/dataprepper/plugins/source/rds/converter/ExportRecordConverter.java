/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_DATABASE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

public class ExportRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ExportRecordConverter.class);

    static final String EXPORT_EVENT_TYPE = "EXPORT";

    public Event convert(final Record<Event> record,
                         final String databaseName,
                         final String tableName,
                         final List<String> primaryKeys,
                         final long eventCreateTimeEpochMillis,
                         final long eventVersionNumber) {
        Event event = record.getData();

        EventMetadata eventMetadata = event.getMetadata();
        eventMetadata.setAttribute(EVENT_DATABASE_NAME_METADATA_ATTRIBUTE, databaseName);
        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableName);
        eventMetadata.setAttribute(BULK_ACTION_METADATA_ATTRIBUTE, OpenSearchBulkActions.INDEX.toString());
        eventMetadata.setAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE, EXPORT_EVENT_TYPE);

        final String primaryKeyValue = primaryKeys.stream()
                .map(key -> event.get(key, String.class))
                .collect(Collectors.joining("|"));
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, primaryKeyValue);

        eventMetadata.setAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreateTimeEpochMillis);
        eventMetadata.setAttribute(EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        return event;
    }
}
