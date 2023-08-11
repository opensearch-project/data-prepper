/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class AnomalyDetectorProcessorConfig {
    @JsonProperty("mode")
    @NotNull
    private PluginModel detectorMode;

    @JsonProperty("keys")
    @NotEmpty
    private List<String> keys;

    @JsonProperty("identification_keys")
    private List<String> identificationKeys;

    @JsonProperty("verbose")
    private Boolean verbose = false;

    public PluginModel getDetectorMode() { 
        return detectorMode;
    }

    public List<String> getKeys() {
        keys.forEach((key) -> {
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Keys cannot be null or empty");
            }
        });
        return keys;
    }

    public List<String> getIdentificationKeys() {
        return identificationKeys;
    }
    public boolean getVerbose() {
        return verbose;
    }


}
