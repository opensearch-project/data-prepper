package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;

public class ActionConfiguration {
    @Getter
    @JsonProperty("type")
    private String type;

    @Getter
    @JsonProperty("when")
    private String when;

    @AssertTrue(message = "type must be one of index, create, update, upsert, delete")
    boolean isTypeValid() {
        if (type.equals("index") || type.equals("create") || type.equals("update") || type.equals("upsert") || type.equals("delete")) {
            return true;
        }
        return false;
    }
}
