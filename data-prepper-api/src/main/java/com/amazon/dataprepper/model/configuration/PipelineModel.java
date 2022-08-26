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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

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
            LOG.warn("Prepper configurations are deprecated, processor configurations will be required in Data Prepper 2.0");
        }

        if (preppers != null && processors != null) {
            final String message = "Pipeline model cannot specify a prepper and processor configuration. It is " +
                    "recommended to move prepper configurations to the processor section to maintain compatibility " +
                    "with DataPrepper version 1.2 and above.";
            throw new IllegalArgumentException(message);
        }
        else if (preppers != null) {
            return preppers;
        }
        else {
            return processors;
        }
    }

    @JsonProperty("source")
    private final PluginModel source;

    @JsonProperty("processor")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<PluginModel> processors;

    @JsonProperty("buffer")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final PluginModel buffer;

    @JsonProperty("router")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<ConditionalRoute> router;

    @JsonProperty("sink")
    private final List<PluginModel> sinks;

    @JsonProperty("workers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer workers;

    @JsonProperty("delay")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer readBatchDelay;

    public PipelineModel(
            final PluginModel source,
            final PluginModel buffer,
            final List<PluginModel> processors,
            final List<ConditionalRoute> router,
            final List<PluginModel> sinks,
            final Integer workers,
            final Integer delay) {
        checkArgument(Objects.nonNull(source), "Source must not be null");
        checkArgument(Objects.nonNull(sinks), "Sinks must not be null");
        checkArgument(sinks.size() > 0, "PipelineModel must include at least 1 sink");


        this.source = source;
        this.buffer = buffer;
        this.processors = processors;
        this.router = router != null ? router : new ArrayList<>();
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
            @JsonProperty("buffer") final PluginModel buffer,
            @Deprecated @JsonProperty("prepper") final List<PluginModel> preppers,
            @JsonProperty("processor") final List<PluginModel> processors,
            @JsonProperty("router") final List<ConditionalRoute> router,
            @JsonProperty("sink") final List<PluginModel> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        this(source, buffer, validateProcessor(preppers, processors), router, sinks, workers, delay);
    }

    public PluginModel getSource() {
        return source;
    }

    public PluginModel getBuffer() { return buffer; }

    @Deprecated
    @JsonIgnore
    public List<PluginModel> getPreppers() {
        return processors;
    }

    public List<PluginModel> getProcessors() {
        return processors;
    }

    public List<ConditionalRoute> getRouter() {
        return router;
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
