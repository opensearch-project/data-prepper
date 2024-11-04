package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LeaderProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper(new JsonFactory())
            .registerModule(new JavaTimeModule());

    @Test
    void testDefaultValues() throws JsonProcessingException {
        String state = "{}";
        LeaderProgressState leaderProgressState = objectMapper.readValue(state, LeaderProgressState.class);
        assertNull(leaderProgressState.getLastPollTime());
        assertFalse(leaderProgressState.isInitialized());
    }

    @Test
    void testInitializedValues() throws JsonProcessingException {
        String state = "{\"last_poll_time\":1729391235717, \"initialized\": true}";
        LeaderProgressState leaderProgressState = objectMapper.readValue(state, LeaderProgressState.class);
        assertEquals(1729391235717000L, leaderProgressState.getLastPollTime().toEpochMilli());
        assertTrue(leaderProgressState.isInitialized());
    }
}
