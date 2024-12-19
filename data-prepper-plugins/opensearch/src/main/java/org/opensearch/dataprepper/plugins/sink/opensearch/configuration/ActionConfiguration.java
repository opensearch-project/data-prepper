package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import org.apache.commons.lang3.EnumUtils;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

public class ActionConfiguration {
    @Getter
    @Size(min = 1, message = "type cannot be empty")
    @JsonProperty("type")
    private String type;

    @Getter
    @JsonProperty("when")
    private String when;

    @AssertTrue(message = "type must be one of index, create, update, upsert, delete")
    boolean isTypeValid() {
        if (EnumUtils.isValidEnumIgnoreCase(OpenSearchBulkActions.class, type)) {
            return true;
        }
        return false;
    }
}
