package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PaginationCrawlerWorkerProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory())
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void testDefaultValues() throws JsonProcessingException {
        PaginationCrawlerWorkerProgressState originalState = new PaginationCrawlerWorkerProgressState();
        String serializedState = objectMapper.writeValueAsString(originalState);
        PaginationCrawlerWorkerProgressState workerProgressState = objectMapper.readValue(serializedState, PaginationCrawlerWorkerProgressState.class);
        assertEquals(0, workerProgressState.getTotalItems());
        assertEquals(0, workerProgressState.getLoadedItems());
        assertNotNull(workerProgressState.getKeyAttributes());
        assertTrue(workerProgressState.getKeyAttributes().isEmpty());
        assertNull(workerProgressState.getExportStartTime());
        assertNull(workerProgressState.getItemIds());
    }

    @Test
    void testInitializedValuesWithIsoInstant() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerWorkerProgressState\",\n" +
                "  \"keyAttributes\": {\"project\": \"project-1\"},\n" +
                "  \"totalItems\": 10,\n" +
                "  \"loadedItems\": 20,\n" +
                "  \"exportStartTime\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"itemIds\": [\"GTMS-25\", \"GTMS-24\"]\n" +
                "}";

        PaginationCrawlerWorkerProgressState workerProgressState = objectMapper.readValue(json, PaginationCrawlerWorkerProgressState.class);
        assertEquals(10, workerProgressState.getTotalItems());
        assertEquals(20, workerProgressState.getLoadedItems());
        assertNotNull(workerProgressState.getKeyAttributes());
        assertEquals("project-1", workerProgressState.getKeyAttributes().get("project"));
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), workerProgressState.getExportStartTime());
        assertNotNull(workerProgressState.getItemIds());
        assertEquals(2, workerProgressState.getItemIds().size());
    }
}
