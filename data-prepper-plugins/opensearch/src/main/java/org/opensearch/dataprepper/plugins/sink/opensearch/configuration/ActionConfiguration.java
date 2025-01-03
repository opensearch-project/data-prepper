package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

public class ActionConfiguration {
    @Size(min = 1, message = "type cannot be empty")
    @JsonProperty("type")
    private OpenSearchBulkActions type;

    public String getType() {
        return type.toString();
    }

    @Getter
    @JsonProperty("when")
    private String when;

}
