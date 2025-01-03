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

    @AssertTrue(message = "type must be one of index, create, update, upsert, delete")
    boolean isTypeValid() {
        if (type == null) {         //type will be null if the string doesnt match one of the enums
            return true;
        }
        return false;
    }

    public String getType() {
        return type.toString();
    }

    @Getter
    @JsonProperty("when")
    private String when;

}
