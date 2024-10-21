package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;


public class LeaderProgressStateTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testDefaultValues() throws JsonProcessingException {
        String state = "{}";
        LeaderProgressState leaderProgressState = objectMapper.readValue(state, LeaderProgressState.class);
        assert leaderProgressState.getLastPollTime() == null;
        assert !leaderProgressState.isInitialized();
    }

    @Test
    void testInitializedValues() throws JsonProcessingException {
        String state = "{\"last_poll_time\":1729391235717, \"initialized\": true}";
        LeaderProgressState leaderProgressState = objectMapper.readValue(state, LeaderProgressState.class);
        assert leaderProgressState.getLastPollTime() == 1729391235717L;
        assert leaderProgressState.isInitialized();
    }
}
