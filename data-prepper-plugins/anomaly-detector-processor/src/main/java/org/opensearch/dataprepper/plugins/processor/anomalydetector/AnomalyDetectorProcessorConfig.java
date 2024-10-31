/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.Collections;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The anomaly detector processor takes structured data and runs anomaly detection algorithms " +
        "on fields that you can configure in that data.")
public class AnomalyDetectorProcessorConfig {
    @JsonPropertyDescription("The ML algorithm (or model) used to detect anomalies. You must provide a mode. See random_cut_forest mode.")
    @JsonProperty("mode")
    @NotNull
    @UsesDataPrepperPlugin(pluginType = AnomalyDetectorMode.class)
    private PluginModel detectorMode;

    @JsonPropertyDescription("A non-ordered List<String> that is used as input to the ML algorithm to detect anomalies in the values of the keys in the list. At least one key is required.")
    @JsonProperty("keys")
    @NotEmpty
    private List<String> keys;

    @JsonPropertyDescription("If provided, anomalies will be detected within each unique instance of these keys. For example, if you provide the ip field, anomalies will be detected separately for each unique IP address.")
    @JsonProperty("identification_keys")
    @ExampleValues({
            @ExampleValues.Example(value = "ip_address", description = "Anomalies will be detected separately for each unique IP address from the existing ip_address key of the Event.")
    })
    private List<String> identificationKeys = Collections.emptyList();

    @JsonPropertyDescription("RCF will try to automatically learn and reduce the number of anomalies detected. For example, if latency is consistently between 50 and 100, and then suddenly jumps to around 1000, only the first one or two data points after the transition will be detected (unless there are other spikes/anomalies). Similarly, for repeated spikes to the same level, RCF will likely eliminate many of the spikes after a few initial ones. This is because the default setting is to minimize the number of alerts detected. Setting the verbose setting to true will cause RCF to consistently detect these repeated cases, which may be useful for detecting anomalous behavior that lasts an extended period of time. Default is false.")
    @JsonProperty("verbose")
    private Boolean verbose = false;

    @JsonPropertyDescription("If using the identification_keys settings, a new ML model will be created for every degree of cardinality. This can cause a large amount of memory usage, so it is helpful to set a limit on the number of models. Default limit is 5000.")
    @JsonProperty(value = "cardinality_limit", defaultValue = "5000")
    private int cardinalityLimit = 5000;

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
    public int getCardinalityLimit() {
        return cardinalityLimit;
    }


}
