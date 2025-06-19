/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import org.opensearch.dataprepper.core.exception.DynamicPipelineConfigUpdateException;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.reflections.Reflections;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DynamicPipelineUpdateUtil {

    public static final Set<String> excludedProcessorsFromDynamicUpdate = Set.of("aggregate");

    public static boolean isDynamicUpdateFeasible(final PipelinesDataFlowModel currentPipelineDataFlowMode,
                                                  final PipelinesDataFlowModel targetDataPipelineDataFlowMode) {

        checkIfSamePipelineExists(currentPipelineDataFlowMode, targetDataPipelineDataFlowMode);
        Set<String> singleThreadedProcessorNames = scanForSingleThreadAnnotatedProcessorPlugins();
        singleThreadedProcessorNames.addAll(excludedProcessorsFromDynamicUpdate);

        for (String pipelineName : currentPipelineDataFlowMode.getPipelines().keySet()) {
            PipelineModel currentPipelineModel = currentPipelineDataFlowMode.getPipelines().get(pipelineName);
            PipelineModel targetPipelineModel = targetDataPipelineDataFlowMode.getPipelines().get(pipelineName);

            // Check if source configuration remains unchanged
            PluginModel currentSource = currentPipelineModel.getSource();
            PluginModel targetSource = targetPipelineModel.getSource();
            if (!currentSource.equals(targetSource)) {
                throw new DynamicPipelineConfigUpdateException(
                        "Source configuration cannot be modified in pipeline: " + pipelineName);
            }

            // Check if sinks configuration remains unchanged
            List<SinkModel> currentSinks = currentPipelineModel.getSinks();
            List<SinkModel> targetSinks = targetPipelineModel.getSinks();
            if (!currentSinks.equals(targetSinks)) {
                throw new DynamicPipelineConfigUpdateException(
                        "Sinks configuration cannot be modified in pipeline: " + pipelineName);
            }

            List<PluginModel> currentProcessors = currentPipelineModel.getProcessors();
            List<PluginModel> targetProcessors = targetPipelineModel.getProcessors();

            currentProcessors = currentProcessors == null ? List.of() : currentProcessors;
            targetProcessors = targetProcessors == null ? List.of() : targetProcessors;

            // Collect single-threaded processors in current and target
            Set<String> currentSingleThreaded = currentProcessors.stream()
                    .map(PluginModel::getPluginName)
                    .filter(singleThreadedProcessorNames::contains)
                    .collect(Collectors.toSet());

            Set<String> targetSingleThreaded = targetProcessors.stream()
                    .map(PluginModel::getPluginName)
                    .filter(singleThreadedProcessorNames::contains)
                    .collect(Collectors.toSet());

            // Only throw if target adds new single-threaded processors or modifies existing ones
            for (String targetProcessor : targetSingleThreaded) {
                if (!currentSingleThreaded.contains(targetProcessor)) {
                    throw new DynamicPipelineConfigUpdateException(
                            "Cannot add new single-threaded processor: " + targetProcessor);
                }
            }
        }
        return true;
    }

    public static void executeDynamicUpdateOfPipelineConfig(final PipelinesDataFlowModel currentPipelineDataFlowMode,
                                                            final PipelinesDataFlowModel targetDataPipelineDataFlowMode) {


    }

    public static void checkIfSamePipelineExists(final PipelinesDataFlowModel currentPipelineDataFlowMode,
                                                 final PipelinesDataFlowModel targetDataPipelineDataFlowMode) {

        Set<String> currentPipelineNames = currentPipelineDataFlowMode.getPipelines().keySet();
        Set<String> targetPipelineNames = targetDataPipelineDataFlowMode.getPipelines().keySet();

        if (!currentPipelineNames.equals(targetPipelineNames)) {
            String addedPipelines = targetPipelineNames.stream()
                    .filter(name -> !currentPipelineNames.contains(name))
                    .collect(Collectors.joining(", "));
            String removedPipelines = currentPipelineNames.stream()
                    .filter(name -> !targetPipelineNames.contains(name))
                    .collect(Collectors.joining(", "));

            StringBuilder errorMessage = new StringBuilder("Pipeline configuration mismatch found.");
            if (!addedPipelines.isEmpty()) {
                errorMessage.append(" New pipelines found: ").append(addedPipelines).append(".");
            }
            if (!removedPipelines.isEmpty()) {
                errorMessage.append(" Missing pipelines: ").append(removedPipelines).append(".");
            }

            throw new DynamicPipelineConfigUpdateException(errorMessage.toString());
        }
    }

    public static Set<String> scanForSingleThreadAnnotatedProcessorPlugins() {
        Reflections reflections = new Reflections("org.opensearch.dataprepper", new TypeAnnotationsScanner());
        Set<Class<?>> dataPrepperPlugins = reflections.getTypesAnnotatedWith(DataPrepperPlugin.class);
        Set<String> singleThreadedProcessorNames = new HashSet<>();

        for (Class<?> clazz : dataPrepperPlugins) {
            if (clazz.isAnnotationPresent(SingleThread.class) || RequiresPeerForwarding.class.isAssignableFrom(clazz)) {
                DataPrepperPlugin pluginAnnotation = clazz.getAnnotation(DataPrepperPlugin.class);
                String name = pluginAnnotation.name();
                singleThreadedProcessorNames.add(name);
            }
        }
        return singleThreadedProcessorNames;
    }

}
