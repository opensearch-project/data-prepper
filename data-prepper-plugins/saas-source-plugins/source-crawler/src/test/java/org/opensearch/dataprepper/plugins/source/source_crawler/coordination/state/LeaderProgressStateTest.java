package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class LeaderProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

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
        assertEquals(Instant.ofEpochMilli(1729391235717L), leaderProgressState.getLastPollTime());
        assertTrue(leaderProgressState.isInitialized());
    }
}
