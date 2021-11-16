/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Model class for a Pipeline containing all possible Plugin types in Configuration YAML
 *
 * @since 1.2
 */
public class PipelineModel {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineModel.class);

    /**
     * @throws IllegalArgumentException If a non-null value is provided to both parameters.
     * Guarantees only a prepper, processor, or neither is provided, not both.
     * @param preppers Deserialized preppers plugin configuration, cannot be used in combination with the processors parameter, nullable
     * @param processors Deserialized processors plugin configuration, cannot be used in combination with the preppers parameter, nullable
     * @return the non-null parameter passed or null if both parameters are null.
     */
    private static List<PluginModel> validateProcessor(
            final List<PluginModel> preppers,
            final List<PluginModel> processors) {
        if (preppers != null) {
            LOG.warn("prepper configurations are deprecated, processor configurations will be required in 1.2");
        }

        if (preppers != null && processors != null) {
            final String message = "Pipeline model cannot specify a prepper and processor configuration. It is " +
                    "recommended to move prepper configurations to the processor section to maintain compatibility " +
                    "with DataPrepper version 1.2 and above.";
            throw new IllegalArgumentException(message);
        }
        else if (preppers != null) {
            LOG.warn("prepper configurations are deprecated, processor configurations will be required in 1.2");
            return preppers;
        }
        else {
            return processors;
        }
    }

    @JsonProperty("source")
    private final PluginModel source;

    @JsonProperty("processor")
    private final List<PluginModel> processors;

    @JsonProperty("sink")
    private final List<PluginModel> sinks;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer workers;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer readBatchDelay;

    public PipelineModel(
            final PluginModel source,
            final List<PluginModel> processors,
            final List<PluginModel> sinks,
            final Integer workers,
            final Integer delay) {
        this.source = source;
        this.processors = processors;
        this.sinks = sinks;
        this.workers = workers;
        this.readBatchDelay = delay;
    }

    /**
     * @since 1.2
     * @param source Deserialized source plugin configuration
     * @param preppers Deserialized preppers plugin configuration, cannot be used in combination with the processors parameter, nullable
     * @param processors Deserialized processors plugin configuration, cannot be used in combination with the preppers parameter, nullable
     * @param sinks Deserialized sinks plugin configuration
     * @param workers Deserialized workers plugin configuration, nullable
     * @param delay Deserialized delay plugin configuration, nullable
     */
    @JsonCreator
    @Deprecated
    public PipelineModel(
            @JsonProperty("source") final PluginModel source,
            @Deprecated @JsonProperty("prepper") final List<PluginModel> preppers,
            @JsonProperty("processor") final List<PluginModel> processors,
            @JsonProperty("sink") final List<PluginModel> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this(source, validateProcessor(preppers, processors), sinks, workers, delay);
    }

    public PluginModel getSource() {
        return source;
    }

    @Deprecated
    @JsonIgnore
    public List<PluginModel> getPreppers() {
        return processors;
    }

    public List<PluginModel> getProcessors() {
        return processors;
    }

    public List<PluginModel> getSinks() {
        return sinks;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }

}
