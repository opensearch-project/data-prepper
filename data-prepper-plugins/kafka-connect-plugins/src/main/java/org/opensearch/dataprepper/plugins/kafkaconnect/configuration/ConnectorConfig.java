package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.List;

public abstract class ConnectorConfig {
    @JsonProperty("force_update")
    public Boolean forceUpdate = false;

    abstract List<Connector> buildConnectors();
}
