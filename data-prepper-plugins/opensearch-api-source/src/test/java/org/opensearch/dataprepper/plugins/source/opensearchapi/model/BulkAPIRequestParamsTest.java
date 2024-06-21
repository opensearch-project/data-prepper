package org.opensearch.dataprepper.plugins.source.opensearchapi.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class BulkAPIRequestParamsTest {

    private static final String testIndex = "test-index";
    private static final String testPipeline = "test-pipeline";
    private static final String testRouting = "test-routing";

    @Test
    public void testValidObjectCreated() {
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder().build();
        assertNull(bulkAPIRequestParams.getIndex());
        assertNull(bulkAPIRequestParams.getPipeline());
        assertNull(bulkAPIRequestParams.getRouting());
    }

    @Test
    public void testValidObjectCreatedWithNonNullFields() {
        BulkAPIRequestParams bulkAPIRequestParams = BulkAPIRequestParams.builder()
                .index(testIndex)
                .pipeline(testPipeline)
                .routing(testRouting)
                .build();
        assertEquals(testIndex, bulkAPIRequestParams.getIndex());
        assertEquals(testPipeline, bulkAPIRequestParams.getPipeline());
        assertEquals(testRouting, bulkAPIRequestParams.getRouting());
    }

    @Test
    public void testValidObjectCreatedWithNonNullFields2() {
        BulkAPIRequestParams bulkAPIRequestParams = new BulkAPIRequestParams(testIndex, testPipeline, testRouting);
        assertEquals(testIndex, bulkAPIRequestParams.getIndex());
        assertEquals(testPipeline, bulkAPIRequestParams.getPipeline());
        assertEquals(testRouting, bulkAPIRequestParams.getRouting());
    }

}
