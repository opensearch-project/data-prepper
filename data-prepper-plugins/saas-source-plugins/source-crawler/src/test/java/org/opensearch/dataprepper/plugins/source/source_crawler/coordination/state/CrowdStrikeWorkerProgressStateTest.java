package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class CrowdStrikeWorkerProgressStateTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    void testDefaultValues() throws JsonProcessingException {
        CrowdStrikeWorkerProgressState originalState = new CrowdStrikeWorkerProgressState();
        String serializedState = objectMapper.writeValueAsString(originalState);
        CrowdStrikeWorkerProgressState state = objectMapper.readValue(serializedState, CrowdStrikeWorkerProgressState.class);
        assertNull(state.getStartTime());
        assertNull(state.getEndTime());
        assertNull(state.getMarker());
    }

    @Test
    void testDeserializeCrowdStrikeWorkerProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState\",\n" +
                "  \"startTime\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"endTime\": \"2024-10-20T03:27:15.717Z\",\n" +
                "  \"marker\": \"2717455246896ffb25d9fcba251f7afa6dad7c1f1ffa\"\n" +
                "}";

        CrowdStrikeWorkerProgressState state = objectMapper.readValue(json, CrowdStrikeWorkerProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getStartTime());
        assertEquals(Instant.parse("2024-10-20T03:27:15.717Z"), state.getEndTime());
        assertEquals("2717455246896ffb25d9fcba251f7afa6dad7c1f1ffa", state.getMarker());
    }
}
