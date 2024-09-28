package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class LeaderProgressState {

    @JsonProperty("initialized")
    private boolean initialized = false;

    @JsonProperty("streamArns")
    private List<String> streamArns;

}
