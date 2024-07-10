/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.converter;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.rds.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

public class ExportRecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ExportRecordConverter.class);

    static final String EXPORT_EVENT_TYPE = "EXPORT";

    public Event convert(Record<Event> record, String tableName, String primaryKeyName) {

        final Event event = JacksonEvent.builder()
                .withEventType(EXPORT_EVENT_TYPE)
                .withData(record.getData().toMap())
                .build();

        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableName);
        eventMetadata.setAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE, EXPORT_EVENT_TYPE);

        final Object primaryKeyValue = record.getData().get(primaryKeyName, Object.class);
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, primaryKeyValue);

        return event;
    }
}
