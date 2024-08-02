/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonCreator;
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
    public static final String SOURCE_PLUGIN_TYPE = "source";
    public static final String PROCESSOR_PLUGIN_TYPE = "processor";
    public static final String BUFFER_PLUGIN_TYPE = "buffer";
    public static final String ROUTE_PLUGIN_TYPE = "route";
    public static final String SINK_PLUGIN_TYPE = "sink";
    private static final Logger LOG = LoggerFactory.getLogger(PipelineModel.class);

    @JsonProperty(SOURCE_PLUGIN_TYPE)
    private final PluginModel source;

    @JsonProperty(PROCESSOR_PLUGIN_TYPE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final List<PluginModel> processors;

    @JsonProperty(BUFFER_PLUGIN_TYPE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final PluginModel buffer;

    @JsonProperty("routes")
    @JsonAlias(ROUTE_PLUGIN_TYPE)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private final List<ConditionalRoute> routes;

    @JsonProperty(SINK_PLUGIN_TYPE)
    private final List<SinkModel> sinks;

    @JsonProperty("workers")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer workers;

    @JsonProperty("delay")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private final Integer readBatchDelay;

    /**
     * @since 2.0
     * @param source Deserialized source plugin configuration
     * @param buffer Deserialized buffer configuration
     * @param processors Deserialized processors plugin configuration, nullable
     * @param routes Deserialized routes configuration, nullable
     * @param sinks Deserialized sinks plugin configuration
     * @param workers Deserialized workers plugin configuration, nullable
     * @param delay Deserialized delay plugin configuration, nullable
     */
    @JsonCreator
    public PipelineModel(
            @JsonProperty("source") final PluginModel source,
            @JsonProperty("buffer") final PluginModel buffer,
            @JsonProperty("processor") final List<PluginModel> processors,
            @JsonProperty("routes")@JsonAlias("route") final List<ConditionalRoute> routes,
            @JsonProperty("sink") final List<SinkModel> sinks,
            @JsonProperty("workers") final Integer workers,
            @JsonProperty("delay") final Integer delay) {
        checkArgument(Objects.nonNull(source), "Source must not be null");
        checkArgument(Objects.nonNull(sinks), "Sinks must not be null");
        checkArgument(sinks.size() > 0, "PipelineModel must include at least 1 sink");


        this.source = source;
        this.buffer = buffer;
        this.processors = processors;
        this.routes = routes != null ? routes : new ArrayList<>();
        this.sinks = sinks;
        this.workers = workers;
        this.readBatchDelay = delay;
    }

    public PluginModel getSource() {
        return source;
    }

    public PluginModel getBuffer() { return buffer; }

    public List<PluginModel> getProcessors() {
        return processors;
    }

    public List<ConditionalRoute> getRoutes() {
        return routes;
    }

    public List<SinkModel> getSinks() {
        return sinks;
    }

    public Integer getWorkers() {
        return workers;
    }

    public Integer getReadBatchDelay() {
        return readBatchDelay;
    }
}
