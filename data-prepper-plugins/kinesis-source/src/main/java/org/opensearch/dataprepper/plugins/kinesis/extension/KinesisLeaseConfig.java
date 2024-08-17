package org.opensearch.dataprepper.plugins.kinesis.extension;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class KinesisLeaseConfig {
    @JsonProperty("lease_coordination")
    private KinesisLeaseCoordinationTableConfig leaseCoordinationTable;
}
