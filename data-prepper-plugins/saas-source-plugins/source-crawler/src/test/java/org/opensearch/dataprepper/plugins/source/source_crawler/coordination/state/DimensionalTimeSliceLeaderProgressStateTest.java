package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DimensionalTimeSliceLeaderProgressStateTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Test
    void testDeserializeDimensionalTimeSliceLeaderProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.DimensionalTimeSliceLeaderProgressState\",\n" +
                "  \"last_poll_time\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"remaining_hours\": 24\n" +
                "}";

        DimensionalTimeSliceLeaderProgressState state = objectMapper.readValue(json, DimensionalTimeSliceLeaderProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getLastPollTime());
        assertEquals(24, state.getRemainingHours());
    }

    @Test
    void testConstructor_setsValuesCorrectly() {
        Instant now = Instant.now();
        DimensionalTimeSliceLeaderProgressState state = new DimensionalTimeSliceLeaderProgressState(now, 48);

        assertNotNull(state);
        assertEquals(now, state.getLastPollTime());
        assertEquals(48, state.getRemainingHours());
    }
}