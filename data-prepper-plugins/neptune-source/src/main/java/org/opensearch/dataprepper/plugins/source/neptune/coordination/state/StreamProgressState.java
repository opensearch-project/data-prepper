package org.opensearch.dataprepper.plugins.source.neptune.coordination.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class StreamProgressState {

    @JsonProperty("startTime")
    private long startTime;

    @JsonProperty("commitNum")
    private Long commitNum;

    @JsonProperty("opNum")
    private Long opNum;

    @JsonProperty("loadedRecords")
    private Long loadedRecords;

    @JsonProperty("lastUpdateTimestamp")
    private long lastUpdateTimestamp;

    @JsonProperty("waitForExport")
    private boolean waitForExport = false;
}
