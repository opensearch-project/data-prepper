package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class TokenPaginationCrawlerLeaderProgressStateTest {

    @Test
    void testDeserializeTokenPaginationCrawlerLeaderProgressState_withTypeInfo() throws JsonProcessingException {
        String json = "{\n" +
                "  \"@class\": \"org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState\",\n" +
                "  \"last_token\": \"sample-token-123\",\n" +
                "  \"initialized\": true\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        TokenPaginationCrawlerLeaderProgressState state = objectMapper.readValue(json, TokenPaginationCrawlerLeaderProgressState.class);
        assertEquals("sample-token-123", state.getLastToken());
        assertEquals(true, state.isInitialized());
    }

    @Test
    void testConstructor_setsLastTokenCorrectly() {
        String testToken = "sample-token-123";
        TokenPaginationCrawlerLeaderProgressState state = new TokenPaginationCrawlerLeaderProgressState(testToken);
        state.setLastPollTime(Instant.now());

        assertEquals(testToken, state.getLastToken());
        assertNotNull(state.getLastPollTime());
    }

}
