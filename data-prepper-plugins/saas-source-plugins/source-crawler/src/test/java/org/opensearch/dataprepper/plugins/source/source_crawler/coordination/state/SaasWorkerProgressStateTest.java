package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SaasWorkerProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory())
            .registerModule(new JavaTimeModule());

    @Test
    void testDefaultValues() throws JsonProcessingException {
        String state = "{}";
        SaasWorkerProgressState workderProgressState = objectMapper.readValue(state, SaasWorkerProgressState.class);
        assertEquals(0, workderProgressState.getTotalItems());
        assertEquals(0, workderProgressState.getLoadedItems());
        assertNotNull(workderProgressState.getKeyAttributes());
        assertTrue(workderProgressState.getKeyAttributes().isEmpty());
        assertNull(workderProgressState.getExportStartTime());
        assertNull(workderProgressState.getItemIds());
    }

    @Test
    void testInitializedValues() throws JsonProcessingException {
        String state = "{\"keyAttributes\":{\"project\":\"project-1\"},\"totalItems\":10,\"loadedItems\":20,\"exportStartTime\":1729391235717,\"itemIds\":[\"GTMS-25\",\"GTMS-24\"]}";
        SaasWorkerProgressState workderProgressState = objectMapper.readValue(state, SaasWorkerProgressState.class);
        assertEquals(10, workderProgressState.getTotalItems());
        assertEquals(20, workderProgressState.getLoadedItems());
        assertNotNull(workderProgressState.getKeyAttributes());
        assertEquals(1729391235717000L, workderProgressState.getExportStartTime().toEpochMilli());
        assertNotNull(workderProgressState.getItemIds());
        assertEquals(2, workderProgressState.getItemIds().size());
    }
}
