/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents an extension of the {@link PluginModel} which is specific to Sink
 * plugins. This model introduces custom properties which are not available as
 * plugin settings.
 *
 * @since 2.0
 */
@JsonSerialize(using = PluginModel.PluginModelSerializer.class)
@JsonDeserialize(using = SinkModel.SinkModelDeserializer.class)
public class SinkModel extends PluginModel {

    SinkModel(final String pluginName, final List<String> routes, final String tagsTargetKey, final Map<String, Object> pluginSettings) {
        this(pluginName, new SinkInternalJsonModel(routes, tagsTargetKey, pluginSettings));
    }

    private SinkModel(final String pluginName, final SinkInternalJsonModel sinkInnerModel) {
        super(pluginName, sinkInnerModel);
    }

    /**
     * Gets the routes associated with this Sink.
     *
     * @return The collection of routes
     * @since 2.0
     */
    public Collection<String> getRoutes() {
        return this.<SinkInternalJsonModel>getInternalJsonModel().routes;
    }

    /**
     * Gets the tags target key associated with this Sink.
     *
     * @return The tags target key
     * @since 2.4
     */
    public String getTagsTargetKey() {
        return this.<SinkInternalJsonModel>getInternalJsonModel().tagsTargetKey;
    }

    public static class SinkModelBuilder {

        private final PluginModel pluginModel;
        private final List<String> routes;
        private final String tagsTargetKey;

        private SinkModelBuilder(final PluginModel pluginModel) {
            this.pluginModel = pluginModel;
            this.routes = Collections.emptyList();
            this.tagsTargetKey = null;
        }

        public SinkModel build() {
            return new SinkModel(pluginModel.getPluginName(), routes, tagsTargetKey, pluginModel.getPluginSettings());
        }
    }

    public static SinkModelBuilder builder(final PluginModel pluginModel) {
        return new SinkModelBuilder(pluginModel);
    }

    private static class SinkInternalJsonModel extends InternalJsonModel {
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonProperty("routes")
        private final List<String> routes;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonProperty("tags_target_key")
        private final String tagsTargetKey;

        @JsonCreator
        private SinkInternalJsonModel(@JsonProperty("routes") final List<String> routes, @JsonProperty("tags_target_key") final String tagsTargetKey) {
            super();
            this.routes = routes != null ? routes : new ArrayList<>();
            this.tagsTargetKey = tagsTargetKey;
        }

        private SinkInternalJsonModel(final List<String> routes, final String tagsTargetKey, final Map<String, Object> pluginSettings) {
            super(pluginSettings);
            this.routes = routes != null ? routes : new ArrayList<>();
            this.tagsTargetKey = tagsTargetKey;
        }
    }

    static class SinkModelDeserializer extends AbstractPluginModelDeserializer<SinkModel, SinkInternalJsonModel> {
        SinkModelDeserializer() {
            super(SinkModel.class, SinkInternalJsonModel.class, SinkModel::new, () -> new SinkInternalJsonModel(null, null));
        }
    }
}
