/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.apache.commons.collections.CollectionUtils;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.parser.model.SinkContextPluginSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class PipelineConfigurationValidator {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineConfigurationValidator.class);
    private static final String PIPELINE_ATTRIBUTE_NAME = "name";
    private static final String PIPELINE_TYPE = "pipeline";
    private static final Set<String> INVALID_PIPELINE_NAMES = new HashSet<>(List.of("data-prepper", "dataPrepper", "core"));

    /**
     * Sorts the pipelines in topological order while also validating for
     * i. cycles in pipeline configuration
     * ii. incorrect pipeline source-sink configuration
     * iii. orphan pipelines [check is disabled for now]
     *
     * @param pipelineConfigurationMap String to PipelineConfiguration map
     * @return List of pipeline names in topological order
     */
    public static List<String> validateAndGetPipelineNames(final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        final Set<String> touchedPipelineSet = new HashSet<>();
        final Set<String> visitedAndProcessedPipelineSet = new HashSet<>();
        final List<String> orderedPipelineNames = new LinkedList<>();

        checkInvalidPipelineNames(pipelineConfigurationMap);

        pipelineConfigurationMap.forEach((pipeline, configuration) -> {
            if (!visitedAndProcessedPipelineSet.contains(pipeline)) {
                visitAndValidate(pipeline, pipelineConfigurationMap, touchedPipelineSet, visitedAndProcessedPipelineSet,
                        orderedPipelineNames);
            }
        });
        Collections.reverse(orderedPipelineNames); // reverse to put the root at the top
        //validateForOrphans(orderedPipelineNames, pipelineConfigurationMap); //TODO: Should we disable orphan pipelines ?
        return orderedPipelineNames;
    }

    private static void checkInvalidPipelineNames(Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        final Set<String> pipelineNames = pipelineConfigurationMap.keySet();

        final Collection<String> invalidPipelineNamesUsed = CollectionUtils.retainAll(pipelineNames, INVALID_PIPELINE_NAMES);
        if (!invalidPipelineNamesUsed.isEmpty()) {
            throw new RuntimeException(format("Cannot use %s as pipeline names.", invalidPipelineNamesUsed));
        }
    }

    private static void visitAndValidate(
            final String pipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Set<String> touchedPipelineSet,
            final Set<String> visitedAndProcessedPipelineSet,
            final List<String> sortedPipelineNames) {

        //if it is already marked, it means it results in a cycle
        if (touchedPipelineSet.contains(pipeline)) {
            LOG.error("Configuration results in a cycle - check pipeline: {}", pipeline);
            throw new RuntimeException(format("Provided configuration results in a loop, check pipeline: %s", pipeline));
        }

        //if its not already visited, recursively check
        if (!visitedAndProcessedPipelineSet.contains(pipeline)) {
            final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipeline);
            touchedPipelineSet.add(pipeline);
            //if validation is successful, then there is definitely sink
            final List<SinkContextPluginSetting> connectedPipelinesSettings = pipelineConfiguration.getSinkPluginSettings();
            //Recursively check connected pipelines
            for (PluginSetting pluginSetting : connectedPipelinesSettings) {
                //Further process only if the sink is of pipeline type
                if (pluginSetting.getName().equals(PIPELINE_TYPE)) {
                    final String connectedPipelineName = (String) pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME);
                    validatePipelineAttributeName(connectedPipelineName, pipeline);
                    validateSourceMapping(pipeline, connectedPipelineName, pipelineConfigurationMap);
                    visitAndValidate(connectedPipelineName, pipelineConfigurationMap, touchedPipelineSet,
                            visitedAndProcessedPipelineSet, sortedPipelineNames);
                }
            }
            visitedAndProcessedPipelineSet.add(pipeline);
            touchedPipelineSet.remove(pipeline);
            sortedPipelineNames.add(pipeline);
        }
    }

    /**
     * This method validates if pipeline's source is correctly configured to reflect the sink of its parent i.e.
     * if p2 is defined as sink for p1, source of p2 should be defined as p1.
     *
     * @param sourcePipeline           name of the expected source pipeline
     * @param currentPipeline          name of the current pipeline that is being validated
     * @param pipelineConfigurationMap pipeline name to pipeline configuration map
     */
    private static void validateSourceMapping(
            final String sourcePipeline,
            final String currentPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        if (!pipelineConfigurationMap.containsKey(currentPipeline)) {
            throw new RuntimeException(format("Invalid configuration, no pipeline is defined with name %s", currentPipeline));
        }
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(currentPipeline);
        final PluginSetting sourcePluginSettings = pipelineConfiguration.getSourcePluginSetting();
        if (!isPipelineAttributeExists(sourcePluginSettings, sourcePipeline)) {
            LOG.error("Invalid configuration, expected source {} for pipeline {} is missing",
                    sourcePipeline, currentPipeline);
            throw new RuntimeException(format("Invalid configuration, expected source %s for pipeline %s is missing",
                    sourcePipeline, currentPipeline));
        }
    }

    private static boolean isPipelineAttributeExists(final PluginSetting pluginSetting, final String pipelineName) {
        boolean result = false;
        if (pluginSetting != null && PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                pipelineName.equals(pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME))) {
            result = true;
        }
        return result;
    }

    private static void validatePipelineAttributeName(final String pipelineAttribute, final String pipelineName) {
        if (pipelineAttribute == null || "".equals(pipelineAttribute.trim())) {
            throw new RuntimeException(format("name is a required attribute for sink pipeline plugin, " +
                    "check pipeline: %s", pipelineName));
        }
    }

    /**
     * Validates for orphan pipeline configurations causing ambiguous execution model.
     * TODO: Should this be removed? (unused code)
     *
     * @param sortedPipelines          pipeline names sorted in reverse order
     * @param pipelineConfigurationMap Map of pipeline name and configuration
     */
    private static void validateForOrphans(
            final List<String> sortedPipelines,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        final Set<String> expectedPipelineSet = new HashSet<>();
        //Add root pipeline name to expected set
        expectedPipelineSet.add(sortedPipelines.get(0));
        for (String currentPipelineName : sortedPipelines) {
            if (!expectedPipelineSet.contains(currentPipelineName)) {
                throw new RuntimeException("Invalid configuration, cannot proceed with ambiguous configuration");
            }
            final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(currentPipelineName);
            final List<SinkContextPluginSetting> pluginSettings = pipelineConfiguration.getSinkPluginSettings();
            for (PluginSetting pluginSetting : pluginSettings) {
                if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                        pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME) != null) {
                    //Add next set of pipeline names to expected set
                    expectedPipelineSet.add((String) pluginSetting.getAttributeFromSettings(PIPELINE_ATTRIBUTE_NAME));
                }
            }
        }
    }

}
