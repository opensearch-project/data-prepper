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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

    public SinkModel(final String pluginName, final List<String> routes, final String tagsTargetKey, final List<String> includeKeys, final List<String> excludeKeys, final Map<String, Object> pluginSettings, final List<PluginModel> response_actions) {
        this(pluginName, new SinkInternalJsonModel(routes, tagsTargetKey, includeKeys, excludeKeys, pluginSettings, response_actions));
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

    public List<PluginModel> getResponseActions() {
        return this.<SinkInternalJsonModel>getInternalJsonModel().response_actions;
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

        private final List<PluginModel> response_actions;

        private SinkModelBuilder(final PluginModel pluginModel) {
            this.pluginModel = pluginModel;
            this.routes = Collections.emptyList();
            this.tagsTargetKey = null;
            this.includeKeys = Collections.emptyList();
            this.excludeKeys = Collections.emptyList();
            this.response_actions = Collections.emptyList();
        }

        public SinkModel build() {
            return new SinkModel(pluginModel.getPluginName(), routes, tagsTargetKey, includeKeys, excludeKeys, pluginModel.getPluginSettings(), response_actions);
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

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        @JsonProperty("response_actions")
        private final List<PluginModel> response_actions;

        @JsonCreator
        private SinkInternalJsonModel(@JsonProperty("routes") final List<String> routes, @JsonProperty("tags_target_key") final String tagsTargetKey,
                                      @JsonProperty("include_keys") final List<String> includeKeys, @JsonProperty("exclude_keys") final List<String> excludeKeys, @JsonProperty("response_actions") final List<PluginModel> responseActions) {
            this(routes, tagsTargetKey, includeKeys, excludeKeys, new HashMap<>(), responseActions);
        }

        private SinkInternalJsonModel(final List<String> routes, final String tagsTargetKey, final List<String> includeKeys, final List<String> excludeKeys, final Map<String, Object> pluginSettings, List<PluginModel> responseActions) {
            super(pluginSettings);
            this.routes = routes != null ? routes : Collections.emptyList();
            this.includeKeys = includeKeys != null ? includeKeys : Collections.emptyList();
            this.excludeKeys = excludeKeys != null ? excludeKeys : Collections.emptyList();
            this.tagsTargetKey = tagsTargetKey;
            response_actions = responseActions;
            validateConfiguration();
            validateKeys();
        }

        void validateConfiguration() {
            if (!includeKeys.isEmpty() && !excludeKeys.isEmpty()) {
                throw new InvalidPluginConfigurationException("include_keys and exclude_keys cannot both exist in the configuration at the same time.");
            }
        }

        /**
         * Validates both include and exclude keys if they contain /
         */
        private void validateKeys() {
            includeKeys.forEach(key -> {
                if(key.contains("/"))
                    throw new InvalidPluginConfigurationException("include_keys cannot contain /");
            });
            excludeKeys.forEach(key -> {
                if(key.contains("/"))
                    throw new InvalidPluginConfigurationException("exclude_keys cannot contain /");
            });
        }
    }


    static class SinkModelDeserializer extends AbstractPluginModelDeserializer<SinkModel, SinkInternalJsonModel> {
        SinkModelDeserializer() {
            super(SinkModel.class, SinkInternalJsonModel.class, SinkModel::new, () -> new SinkInternalJsonModel(null, null, null, null, null));
        }
    }
}