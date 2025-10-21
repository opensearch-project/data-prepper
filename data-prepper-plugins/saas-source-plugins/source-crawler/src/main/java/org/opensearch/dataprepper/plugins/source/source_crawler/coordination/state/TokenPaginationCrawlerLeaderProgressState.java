package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import org.opensearch.dataprepper.plugins.source.source_crawler.base.TokenLeaderProgressState;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.LeaderProgressState;

import java.time.Instant;

@Data
@NoArgsConstructor
public class TokenPaginationCrawlerLeaderProgressState implements TokenLeaderProgressState, LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("last_token")
    private String lastToken;

    private Instant lastPollTime;

    public TokenPaginationCrawlerLeaderProgressState(@JsonProperty("last_token") final String lastToken) {
        this.lastToken = lastToken;
    }
}

