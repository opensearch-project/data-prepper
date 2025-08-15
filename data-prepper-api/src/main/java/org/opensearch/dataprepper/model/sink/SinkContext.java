/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.model.pipeline.HeadlessPipeline;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data Prepper Sink Context class. This the class for keeping global
 * sink configuration as context so that individual sinks may use them.
 */
public class SinkContext {
    private final String tagsTargetKey;
    private final Collection<String> routes;
    private final List<String> includeKeys;
    private final List<String> excludeKeys;
    private Map<String, HeadlessPipeline> forwardToPipelines;

    public SinkContext(String tagsTargetKey, Collection<String> routes, List<String> includeKeys, List<String> excludeKeys) {
        this.tagsTargetKey = tagsTargetKey;
        this.routes = routes;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.forwardToPipelines = new HashMap<>();
    }

    public SinkContext(String tagsTargetKey, Collection<String> routes, List<String> includeKeys, List<String> excludeKeys, List<String> forwardPipelineNames) {
        this.tagsTargetKey = tagsTargetKey;
        this.routes = routes;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.forwardToPipelines = new HashMap<>();
        if (forwardPipelineNames != null) {
            for (final String forwardPipelineName: forwardPipelineNames) {
                this.forwardToPipelines.put(forwardPipelineName, null);
            }
        }
    }

    public SinkContext(String tagsTargetKey) {
        this(tagsTargetKey, null, null, null, null);
    }

    public void setForwardToPipelines(final Map<String, HeadlessPipeline> pipelines) {
        for (Map.Entry<String, HeadlessPipeline> entry: forwardToPipelines.entrySet()) {
            final String key = entry.getKey();
            final HeadlessPipeline pipeline = pipelines.get(key);
            if (pipeline != null) {
                forwardToPipelines.put(key, pipeline);
            } else {
                throw new RuntimeException(String.format("forwarding pipeline {} doesn't exist", key));
            }
        }
    }

    public boolean forwardRecords(final Collection<Record<Event>> records) {
        if (forwardToPipelines.size() == 0) {
            return false;
        }
        
        for (Map.Entry<String, HeadlessPipeline> entry: forwardToPipelines.entrySet()) {
            if (entry.getValue() == null) {
                return false;
            }
        }

        for (Map.Entry<String, HeadlessPipeline> entry: forwardToPipelines.entrySet()) {
            entry.getValue().sendEvents(records);
        }
        return true;
    }

    /**
     * returns the target key name for tags if configured for a given sink
     *
     * @return tags target key
     */
    public String getTagsTargetKey() {
        return tagsTargetKey;
    }

    /**
     * returns routes if configured for a given sink
     *
     * @return routes
     */
    public Collection<String> getRoutes() {
        return routes;
    }

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public Map<String, HeadlessPipeline> getForwardToPipelines() {
        return forwardToPipelines;
    }
}

