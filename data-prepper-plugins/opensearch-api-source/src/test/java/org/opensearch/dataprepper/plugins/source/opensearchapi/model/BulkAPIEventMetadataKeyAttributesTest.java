package org.opensearch.dataprepper.plugins.source.opensearchapi.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BulkAPIEventMetadataKeyAttributesTest {

    @Test
    public void testEventMetadataKeyAttributes() {
        assertEquals(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION, "opensearch_action");
        assertEquals(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX, "opensearch_index");
        assertEquals(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ID, "opensearch_id");
        assertEquals(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_PIPELINE, "opensearch_pipeline");
        assertEquals(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ROUTING, "opensearch_routing");
    }
}
