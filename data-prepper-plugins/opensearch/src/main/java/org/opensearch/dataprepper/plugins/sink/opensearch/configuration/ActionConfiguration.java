/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;

public class ActionConfiguration {
    @JsonProperty("type")
    private OpenSearchBulkActions type;

    @AssertTrue(message = "action must be one of index, create, update, upsert, delete")
    boolean isActionValid() {
        if (type == null) {         //type will be null if the string doesn't match one of the enums
            return false;
        }
        return true;
    }

    public String getType() {
        return type.toString();
    }

    @Getter
    @JsonProperty("when")
    private String when;

}
