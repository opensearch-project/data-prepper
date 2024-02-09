/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flattenjson;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class FlattenJsonProcessorConfig {

    @NotEmpty
    @NotNull
    @JsonProperty("source")
    private String source;

    @NotEmpty
    @NotNull
    @JsonProperty("target")
    private String target;

    @JsonProperty("remove_processed_fields")
    private boolean removeProcessedFields;

    @JsonProperty("flatten_when")
    private String flattenWhen;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public boolean getRemoveProcessedFields() {
        return removeProcessedFields;
    }

    public String getFlattenWhen() {
        return flattenWhen;
    }
}
