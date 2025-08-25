package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class DimensionalTimeSliceWorkerProgressStateTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void testDefaultValues() throws JsonProcessingException {
        DimensionalTimeSliceWorkerProgressState originalState = new DimensionalTimeSliceWorkerProgressState();
        String serializedState = objectMapper.writeValueAsString(originalState);
        DimensionalTimeSliceWorkerProgressState state = objectMapper.readValue(serializedState, DimensionalTimeSliceWorkerProgressState.class);

        assertNull(state.getStartTime());
        assertNull(state.getEndTime());
        assertNull(state.getDimensionType());
    }

    @Test
    void testDeserializeDimensionalTimeSliceWorkerProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceWorkerProgressState\",\n" +
                "  \"startTime\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"endTime\": \"2024-10-20T03:27:15.717Z\",\n" +
                "  \"dimensionType\": \"Exchange\"\n" +
                "}";

        DimensionalTimeSliceWorkerProgressState state = objectMapper.readValue(json, DimensionalTimeSliceWorkerProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getStartTime());
        assertEquals(Instant.parse("2024-10-20T03:27:15.717Z"), state.getEndTime());
        assertEquals("Exchange", state.getDimensionType());
    }
}
