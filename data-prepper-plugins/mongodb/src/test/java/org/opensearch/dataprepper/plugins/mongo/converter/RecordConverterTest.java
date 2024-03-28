/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.mongo.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;

@ExtendWith(MockitoExtension.class)
class RecordConverterTest {
    private CollectionConfig collectionConfig;

    final Random random = new Random();

    @BeforeEach
    void setup() throws Exception {
        collectionConfig = new CollectionConfig();
        ReflectivelySetField.setField(CollectionConfig.class, collectionConfig, "collection", "staging.products");
    }

    @Test
    void convert() {
        final String id = UUID.randomUUID().toString();
        final String record = "{" +
                "\"_id\":\"" + id + "\"," +
                "\"customerId\":" + random.nextInt() + "," +
                "\"productId\":" + random.nextInt() + "," +
                "\"quantity\":" + random.nextInt() + "," +
                "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}";
        final long exportStartTime = Instant.now().toEpochMilli();
        final long eventVersionNumber = random.nextLong();

        final RecordConverter recordConverter = new RecordConverter(collectionConfig, ExportPartition.PARTITION_TYPE);

        final JacksonEvent event = (JacksonEvent) recordConverter.convert(record, exportStartTime, eventVersionNumber);
        assertThat(event.getMetadata(), notNullValue());

        assertThat(event.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(event.getMetadata().getAttribute(MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE), notNullValue());
        assertThat(event.getMetadata().getAttribute(MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), nullValue());
        assertThat(event.getMetadata().getAttribute(MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(exportStartTime));
        assertThat(event.getMetadata().getAttribute(EVENT_VERSION_FROM_TIMESTAMP), equalTo(eventVersionNumber));
        assertThat(event.getMetadata().getAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo(ExportPartition.PARTITION_TYPE));
        assertThat(event.getEventHandle(), notNullValue());
        assertThat(event.getEventHandle().getExternalOriginationTime(), nullValue());
    }

    @Test
    void convertWithEventName() {
        final String id = UUID.randomUUID().toString();
        final String record = "{" +
                "\"_id\":\"" + id + "\"," +
                "\"customerId\":" + random.nextInt() + "," +
                "\"productId\":" + random.nextInt() + "," +
                "\"quantity\":" + random.nextInt() + "," +
                "\"orderDate\":{\"date\":\"" + LocalDate.now() +"\"}}";
        final long exportStartTime = Instant.now().toEpochMilli();
        final long eventVersionNumber = random.nextLong();
        final String eventName = "insert";

        final RecordConverter recordConverter = new RecordConverter(collectionConfig, StreamPartition.PARTITION_TYPE);

        final JacksonEvent event = (JacksonEvent) recordConverter.convert(record, exportStartTime, eventVersionNumber, eventName);
        assertThat(event.getMetadata(), notNullValue());

        assertThat(event.getMetadata().getAttribute(PARTITION_KEY_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE), equalTo(id));
        assertThat(event.getMetadata().getAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo(OpenSearchBulkActions.INDEX.toString()));
        assertThat(event.getMetadata().getAttribute(MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE), notNullValue());
        assertThat(event.getMetadata().getAttribute(MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE), equalTo(eventName));
        assertThat(event.getMetadata().getAttribute(MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(exportStartTime));
        assertThat(event.getMetadata().getAttribute(EVENT_VERSION_FROM_TIMESTAMP), equalTo(eventVersionNumber));
        assertThat(event.getMetadata().getAttribute(INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo(StreamPartition.PARTITION_TYPE));

        assertThat(event.getEventHandle(), notNullValue());
        assertThat(event.getEventHandle().getExternalOriginationTime(), equalTo(Instant.ofEpochMilli(exportStartTime)));
        assertThat(event.getMetadata().getExternalOriginationTime(), equalTo(Instant.ofEpochMilli(exportStartTime)));
    }
}