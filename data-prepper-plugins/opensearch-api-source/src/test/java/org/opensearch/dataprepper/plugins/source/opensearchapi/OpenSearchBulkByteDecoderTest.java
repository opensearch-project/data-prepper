/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIEventMetadataKeyAttributes;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenSearchBulkByteDecoderTest {

    private OpenSearchBulkByteDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder = new OpenSearchBulkByteDecoder();
    }

    @Test
    void parse_withIndexAction_createsEventWithCorrectMetadata() throws IOException {
        String bulk = "{\"index\":{\"_index\":\"my-index\",\"_id\":\"1\"}}\n" +
                "{\"field1\":\"value1\",\"field2\":42}\n";

        List<Record<Event>> records = parseAll(bulk);

        assertEquals(1, records.size());
        Event event = records.get(0).getData();
        assertEquals("value1", event.get("field1", String.class));
        assertEquals(42, event.get("field2", Integer.class));
        assertEquals("index", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
        assertEquals("my-index", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX));
        assertEquals("1", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ID));
    }

    @Test
    void parse_withDeleteAction_createsEventWithNoBody() throws IOException {
        String bulk = "{\"delete\":{\"_index\":\"my-index\",\"_id\":\"2\"}}\n";

        List<Record<Event>> records = parseAll(bulk);

        assertEquals(1, records.size());
        Event event = records.get(0).getData();
        assertEquals("delete", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
        assertEquals("my-index", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX));
    }

    @Test
    void parse_withMultipleActions_createsMultipleEvents() throws IOException {
        String bulk = "{\"index\":{\"_index\":\"idx\",\"_id\":\"1\"}}\n" +
                "{\"name\":\"doc1\"}\n" +
                "{\"create\":{\"_index\":\"idx\",\"_id\":\"2\"}}\n" +
                "{\"name\":\"doc2\"}\n" +
                "{\"delete\":{\"_index\":\"idx\",\"_id\":\"3\"}}\n";

        List<Record<Event>> records = parseAll(bulk);

        assertEquals(3, records.size());
        assertEquals("index", records.get(0).getData().getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
        assertEquals("create", records.get(1).getData().getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
        assertEquals("delete", records.get(2).getData().getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
    }

    @Test
    void parse_withRoutingAndPipeline_setsMetadata() throws IOException {
        String bulk = "{\"index\":{\"_index\":\"idx\",\"routing\":\"r1\",\"pipeline\":\"p1\"}}\n" +
                "{\"data\":true}\n";

        List<Record<Event>> records = parseAll(bulk);

        assertEquals(1, records.size());
        Event event = records.get(0).getData();
        assertEquals("r1", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ROUTING));
        assertEquals("p1", event.getMetadata().getAttribute(
                BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_PIPELINE));
    }

    @Test
    void parse_withBlankLines_skipsBlankLines() throws IOException {
        String bulk = "\n\n{\"index\":{\"_index\":\"idx\"}}\n{\"x\":1}\n\n";

        List<Record<Event>> records = parseAll(bulk);

        assertEquals(1, records.size());
    }

    @Test
    void parse_withEmptyInput_returnsNoEvents() throws IOException {
        List<Record<Event>> records = parseAll("");
        assertTrue(records.isEmpty());
    }

    @Test
    void parse_withTimeReceived_setsTimeOnEvent() throws IOException {
        String bulk = "{\"index\":{\"_index\":\"idx\"}}\n{\"x\":1}\n";
        Instant now = Instant.now();

        List<Record<Event>> records = new ArrayList<>();
        decoder.parse(new ByteArrayInputStream(bulk.getBytes(StandardCharsets.UTF_8)), now, records::add);

        assertEquals(1, records.size());
        assertNotNull(records.get(0).getData().getMetadata().getTimeReceived());
    }

    private List<Record<Event>> parseAll(String input) throws IOException {
        List<Record<Event>> records = new ArrayList<>();
        decoder.parse(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), Instant.now(), records::add);
        return records;
    }
}
