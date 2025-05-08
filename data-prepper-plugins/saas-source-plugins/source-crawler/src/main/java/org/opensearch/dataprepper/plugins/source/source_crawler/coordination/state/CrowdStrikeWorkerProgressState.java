package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.SaasWorkerProgressState;

import java.time.Instant;

@Data
public class CrowdStrikeWorkerProgressState implements SaasWorkerProgressState {

    @JsonProperty("startTime")
    private Instant startTime;

    @JsonProperty("endTime")
    private Instant endTime;

    @JsonProperty("marker")
    private String marker;
}
