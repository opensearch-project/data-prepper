package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.TokenPaginationCrawlerLeaderProgressState;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
@JsonSubTypes({
        @JsonSubTypes.Type(value = TokenPaginationCrawlerLeaderProgressState.class)
})
public interface  TokenLeaderProgressState {

    String getLastToken();

    void setLastToken(String token);
}

