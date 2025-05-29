package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class CrowdStrikeLeaderProgressStateTest {

    @Test
    void testDeserializeCrowdStrikeLeaderProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState\",\n" +
                "  \"last_poll_time\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"remaining_days\": 3\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        CrowdStrikeLeaderProgressState state = objectMapper.readValue(json, CrowdStrikeLeaderProgressState.class);
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getLastPollTime());
        assertEquals(3, state.getRemainingDays());
    }

    @Test
    void testConstructor_setsLastPollTimeCorrectly() {
        Instant now = Instant.now();
        CrowdStrikeLeaderProgressState state = new CrowdStrikeLeaderProgressState(now, 10);
        assertEquals(now, state.getLastPollTime());
        assertEquals(10, state.getRemainingDays());
    }

}
