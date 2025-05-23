package org.opensearch.dataprepper.plugins.source.source_crawler.base;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.PaginationCrawlerLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState;

import java.time.Instant;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PaginationCrawlerLeaderProgressState.class),
        @JsonSubTypes.Type(value = CrowdStrikeLeaderProgressState.class)
})
public interface LeaderProgressState {

    Instant getLastPollTime();

    void setLastPollTime(Instant lastPollTime);
}
