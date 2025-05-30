package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class PaginationCrawlerLeaderProgressStateTest {

    @Test
    void testInitializedValuesWithIsoInstant() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerLeaderProgressState\",\n" +
                "  \"last_poll_time\": \"2024-10-20T02:27:15.717Z\",\n" +
                "  \"initialized\": true\n" +
                "}";

        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        PaginationCrawlerLeaderProgressState state = mapper.readValue(json, PaginationCrawlerLeaderProgressState.class);

        assertTrue(state.isInitialized());
        assertEquals(Instant.parse("2024-10-20T02:27:15.717Z"), state.getLastPollTime());
    }

    @Test
    void testConstructor_setsLastPollTimeCorrectly() {
        Instant now = Instant.now();
        PaginationCrawlerLeaderProgressState state = new PaginationCrawlerLeaderProgressState(now);
        assertEquals(now, state.getLastPollTime());
        assertFalse(state.isInitialized(), "Expected 'initialized' to be false by default");
    }

}
