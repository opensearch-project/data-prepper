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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    SinkModel(final String pluginName, final List<String> routes, final String tagsTargetKey, final List<String> includeKeys, final List<String> excludeKeys, final Map<String, Object> pluginSettings) {
        this(pluginName, new SinkInternalJsonModel(routes, tagsTargetKey, includeKeys, excludeKeys, pluginSettings));
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

    public List<String> getIncludeKeys() {
        return this.<SinkInternalJsonModel>getInternalJsonModel().includeKeys;
    }

    public List<String> getExcludeKeys() {
        return this.<SinkInternalJsonModel>getInternalJsonModel().excludeKeys;
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

        private final List<String> includeKeys;
        private final List<String> excludeKeys;

        private SinkModelBuilder(final PluginModel pluginModel) {
            this.pluginModel = pluginModel;
            this.routes = Collections.emptyList();
            this.tagsTargetKey = null;
            this.includeKeys = Collections.emptyList();
            this.excludeKeys = Collections.emptyList();
        }

        public SinkModel build() {
            return new SinkModel(pluginModel.getPluginName(), routes, tagsTargetKey, includeKeys, excludeKeys, pluginModel.getPluginSettings());
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


        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonProperty("include_keys")
        private final List<String> includeKeys;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonProperty("exclude_keys")
        private final List<String> excludeKeys;

        @JsonCreator
        private SinkInternalJsonModel(@JsonProperty("routes") final List<String> routes, @JsonProperty("tags_target_key") final String tagsTargetKey, @JsonProperty("include_keys") final List<String> includeKeys, @JsonProperty("exclude_keys") final List<String> excludeKeys) {
            this(routes, tagsTargetKey, includeKeys, excludeKeys, new HashMap<>());
        }

        private SinkInternalJsonModel(final List<String> routes, final String tagsTargetKey, final List<String> includeKeys, final List<String> excludeKeys, final Map<String, Object> pluginSettings) {
            super(pluginSettings);
            this.routes = routes != null ? routes : Collections.emptyList();
            this.includeKeys = includeKeys != null ? preprocessingKeys(includeKeys) : Collections.emptyList();
            this.excludeKeys = excludeKeys != null ? preprocessingKeys(excludeKeys) : Collections.emptyList();
            this.tagsTargetKey = tagsTargetKey;
        }


        /**
         * Pre-processes a list of Keys and returns a sorted list
         * The keys must start with `/` and not end with `/`
         *
         * @param keys a list of raw keys
         * @return a sorted processed keys
         */
        private List<String> preprocessingKeys(final List<String> keys) {
            if (keys.contains("/")) {
                return new ArrayList<>();
            }
            List<String> result = keys.stream()
                    .map(k -> k.startsWith("/") ? k : "/" + k)
                    .map(k -> k.endsWith("/") ? k.substring(0, k.length() - 1) : k)
                    .collect(Collectors.toList());
            Collections.sort(result);
            return result;
        }
    }


    static class SinkModelDeserializer extends AbstractPluginModelDeserializer<SinkModel, SinkInternalJsonModel> {
        SinkModelDeserializer() {
            super(SinkModel.class, SinkInternalJsonModel.class, SinkModel::new, () -> new SinkInternalJsonModel(null, null, null, null));
        }
    }
}
