/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.converter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.source.neptune.stream.model.NeptuneStreamRecord;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.neptunedata.model.PropertygraphData;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(MockitoExtension.class)
class StreamRecordConverterTest {
    private NeptuneStreamRecord record;

    @BeforeEach
    void setup() {
        PropertygraphData data = PropertygraphData.builder()
                .key("label")
                .value(Document.fromMap(Map.of("value", Document.fromString("a"))))
                .type("v")
                .id("i")
                .build();
        record = NeptuneStreamRecord.builder().data(data).op("ADD").id("i").commitTimestampInMillis(1L).build();
    }

    @Test
    void convert() {
        final StreamRecordConverter recordConverter = new StreamRecordConverter("a");
        recordConverter.setPartitions(List.of("a"));

        final JacksonEvent event = (JacksonEvent) recordConverter.convert(record);
        assertThat(event.getMetadata(), notNullValue());

        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.ID_METADATA_ATTRIBUTE), equalTo("i"));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.NEPTUNE_COMMIT_TIMESTAMP_METADATA_ATTRIBUTE), equalTo(1L));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP), equalTo(1L));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.NEPTUNE_STREAM_OP_NAME_METADATA_ATTRIBUTE), equalTo("ADD"));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE), equalTo("upsert"));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE), equalTo("STREAM"));
        assertThat(event.getMetadata().getAttribute(MetadataKeyAttributes.EVENT_S3_PARTITION_KEY), equalTo("a/a"));
        assertThat(event.getEventHandle(), notNullValue());
        assertThat(event.getEventHandle().getExternalOriginationTime(), nullValue());
    }
}