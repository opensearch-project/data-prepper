/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser;

import org.opensearch.dataprepper.core.breaker.CircuitBreakerManager;
import org.opensearch.dataprepper.core.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderConfiguration;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwarderProvider;
import org.opensearch.dataprepper.core.peerforwarder.PeerForwardingProcessorDecorator;
import org.opensearch.dataprepper.core.pipeline.Pipeline;
import org.opensearch.dataprepper.core.pipeline.PipelineConnector;
import org.opensearch.dataprepper.core.pipeline.PipelineRunnerImpl;
import org.opensearch.dataprepper.core.pipeline.SupportsPipelineRunner;
import org.opensearch.dataprepper.core.pipeline.router.Router;
import org.opensearch.dataprepper.core.pipeline.router.RouterFactory;
import org.opensearch.dataprepper.core.sourcecoordination.SourceCoordinatorFactory;
import org.opensearch.dataprepper.core.validation.PluginErrorCollector;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationValidator;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.pipeline.parser.model.SinkContextPluginSetting;
import org.opensearch.dataprepper.validation.PluginError;
import org.opensearch.dataprepper.validation.PluginErrorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;

@SuppressWarnings("rawtypes")
public class PipelineTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineTransformer.class);

    static final String CONDITIONAL_ROUTE_INVALID_EXPRESSION_FORMAT = "Route %s contains an invalid conditional expression '%s'. " +
            "See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax.";
    private static final String PIPELINE_TYPE = "pipeline";
    private static final String ATTRIBUTE_NAME = "name";
    private final PipelinesDataFlowModel pipelinesDataFlowModel;
    private final RouterFactory routerFactory;
    private final DataPrepperConfiguration dataPrepperConfiguration;
    private final CircuitBreakerManager circuitBreakerManager;
    private final Map<String, PipelineConnector> sourceConnectorMap = new HashMap<>(); //TODO Remove this and rely only on pipelineMap
    private final PluginFactory pluginFactory;
    private final PeerForwarderProvider peerForwarderProvider;
    private final EventFactory eventFactory;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final SourceCoordinatorFactory sourceCoordinatorFactory;
    private final PluginErrorCollector pluginErrorCollector;
    private final PluginErrorsHandler pluginErrorsHandler;

    private final ExpressionEvaluator expressionEvaluator;

    public PipelineTransformer(final PipelinesDataFlowModel pipelinesDataFlowModel,
                               final PluginFactory pluginFactory,
                               final PeerForwarderProvider peerForwarderProvider,
                               final RouterFactory routerFactory,
                               final DataPrepperConfiguration dataPrepperConfiguration,
                               final CircuitBreakerManager circuitBreakerManager,
                               final EventFactory eventFactory,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final SourceCoordinatorFactory sourceCoordinatorFactory,
                               final PluginErrorCollector pluginErrorCollector,
                               final PluginErrorsHandler pluginErrorsHandler,
                               final ExpressionEvaluator expressionEvaluator) {
        this.pipelinesDataFlowModel = pipelinesDataFlowModel;
        this.pluginFactory = Objects.requireNonNull(pluginFactory);
        this.peerForwarderProvider = Objects.requireNonNull(peerForwarderProvider);
        this.routerFactory = routerFactory;
        this.dataPrepperConfiguration = Objects.requireNonNull(dataPrepperConfiguration);
        this.circuitBreakerManager = circuitBreakerManager;
        this.eventFactory = eventFactory;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceCoordinatorFactory = sourceCoordinatorFactory;
        this.pluginErrorCollector = pluginErrorCollector;
        this.pluginErrorsHandler = pluginErrorsHandler;
        this.expressionEvaluator = expressionEvaluator;
    }

    public Map<String, Pipeline> transformConfiguration() {
        final Map<String, PipelineConfiguration> pipelineConfigurationMap = pipelinesDataFlowModel.getPipelines().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new PipelineConfiguration(entry.getValue())
                ));

        final List<String> allPipelineNames = PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigurationMap);

        // LinkedHashMap to preserve insertion order
        final Map<String, Pipeline> pipelineMap = new LinkedHashMap<>();
        pipelineConfigurationMap.forEach((pipelineName, configuration) ->
                configuration.updateCommonPipelineConfiguration(pipelineName));
        for (String pipelineName : allPipelineNames) {
            if (!pipelineMap.containsKey(pipelineName) && pipelineConfigurationMap.containsKey(pipelineName)) {
                buildPipelineFromConfiguration(pipelineName, pipelineConfigurationMap, pipelineMap);
            }
        }
        return pipelineMap;
    }

    private void buildPipelineFromConfiguration(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
        LOG.info("Building pipeline [{}] from provided configuration", pipelineName);
        try {
            final PluginSetting sourceSetting = pipelineConfiguration.getSourcePluginSetting();
            final Optional<Source> pipelineSource = getSourceIfPipelineType(pipelineName, sourceSetting,
                    pipelineMap, pipelineConfigurationMap);
            final Source source = pipelineSource.orElseGet(() -> {
                try {
                    return pluginFactory.loadPlugin(Source.class, sourceSetting);
                } catch (Exception e) {
                    final PluginError pluginError = PluginError.builder()
                            .componentType(PipelineModel.SOURCE_PLUGIN_TYPE)
                            .pipelineName(pipelineName)
                            .pluginName(sourceSetting.getName())
                            .exception(e)
                            .build();
                    pluginErrorCollector.collectPluginError(pluginError);
                    return null;
                }
            });

            LOG.info("Building buffer for the pipeline [{}]", pipelineName);
            Buffer pipelineDefinedBuffer = null;
            final PluginSetting bufferPluginSetting = pipelineConfiguration.getBufferPluginSetting();
            try {
                if (source != null) {
                    pipelineDefinedBuffer = pluginFactory.loadPlugin(Buffer.class, bufferPluginSetting, source.getDecoder());
                }
            } catch (Exception e) {
                final PluginError pluginError = PluginError.builder()
                        .componentType(PipelineModel.BUFFER_PLUGIN_TYPE)
                        .pipelineName(pipelineName)
                        .pluginName(bufferPluginSetting.getName())
                        .exception(e)
                        .build();
                pluginErrorCollector.collectPluginError(pluginError);
            }

            LOG.info("Building processors for the pipeline [{}]", pipelineName);
            final int processorThreads = pipelineConfiguration.getWorkers();

            final List<List<IdentifiedComponent<Processor>>> processorSets = pipelineConfiguration.getProcessorPluginSettings().stream()
                    .map(this::newProcessor)
                    .collect(Collectors.toList());

            LOG.info("Building sinks for the pipeline [{}]", pipelineName);
            final List<DataFlowComponent<Sink>> sinks = pipelineConfiguration.getSinkPluginSettings().stream()
                    .map(this::buildRoutedSinkOrConnector)
                    .collect(Collectors.toList());

            final List<PluginError> invalidRouteExpressions = pipelineConfiguration.getRoutes()
                    .stream().filter(route -> !expressionEvaluator.isValidExpressionStatement(route.getCondition()))
                    .map(route -> PluginError.builder()
                            .componentType(PipelineModel.ROUTE_PLUGIN_TYPE)
                            .pipelineName(pipelineName)
                            .exception(new InvalidPluginConfigurationException(
                                    String.format(CONDITIONAL_ROUTE_INVALID_EXPRESSION_FORMAT, route.getName(), route.getCondition())))
                            .build())
                    .collect(Collectors.toList());

            invalidRouteExpressions.forEach(pluginErrorCollector::collectPluginError);
            final List<PluginError> subPipelinePluginErrors = pluginErrorCollector.getPluginErrors()
                    .stream().filter(pluginError -> pipelineName.equals(pluginError.getPipelineName()))
                    .collect(Collectors.toList());

            if (!subPipelinePluginErrors.isEmpty()) {
                pluginErrorsHandler.handleErrors(subPipelinePluginErrors);
                throw new InvalidPluginConfigurationException(
                        String.format("One or more plugins are not configured correctly in the pipeline: %s.\n",
                                pipelineName));
            }

            final List<List<Processor>> decoratedProcessorSets = processorSets.stream()
                    .map(processorComponentList -> {
                        final List<Processor> processors = processorComponentList.stream().map(IdentifiedComponent::getComponent).collect(Collectors.toList());
                        if (!processors.isEmpty() && processors.get(0) instanceof RequiresPeerForwarding) {
                            return PeerForwardingProcessorDecorator.decorateProcessors(
                                    processors, peerForwarderProvider, pipelineName, processorComponentList.get(0).getName(),
                                    dataPrepperConfiguration.getPeerForwarderConfiguration() != null ?
                                        dataPrepperConfiguration.getPeerForwarderConfiguration().getExcludeIdentificationKeys() : null,
                                    pipelineConfiguration.getWorkers()
                            );
                        }
                        return processors;
                    }).collect(Collectors.toList());

            final int readBatchDelay = pipelineConfiguration.getReadBatchDelay();

            final List<Buffer> secondaryBuffers = getSecondaryBuffers();
            LOG.info("Constructing MultiBufferDecorator with [{}] secondary buffers for pipeline [{}]", secondaryBuffers.size(), pipelineName);
            final MultiBufferDecorator multiBufferDecorator = new MultiBufferDecorator(pipelineDefinedBuffer, secondaryBuffers);


            final Buffer buffer = applyCircuitBreakerToBuffer(source, multiBufferDecorator);

            final Router router = routerFactory.createRouter(pipelineConfiguration.getRoutes());

            final Pipeline pipeline = new Pipeline(pipelineName, source, buffer, decoratedProcessorSets, sinks, router,
                    eventFactory, acknowledgementSetManager, sourceCoordinatorFactory, processorThreads, readBatchDelay,
                    dataPrepperConfiguration.getProcessorShutdownTimeout(), dataPrepperConfiguration.getSinkShutdownTimeout(),
                    getPeerForwarderDrainTimeout(dataPrepperConfiguration));

            if (pipelineDefinedBuffer instanceof SupportsPipelineRunner) {
                ((SupportsPipelineRunner) pipelineDefinedBuffer).setPipelineRunner(new PipelineRunnerImpl(pipeline));
            }

            pipelineMap.put(pipelineName, pipeline);
        } catch (Exception ex) {
            //If pipeline construction errors out, we will skip that pipeline and proceed
            LOG.error("Construction of pipeline components failed, skipping building of pipeline [{}] and its connected " +
                    "pipelines", pipelineName, ex);
            processRemoveIfRequired(pipelineName, pipelineConfigurationMap, pipelineMap);
        }

    }

    private List<IdentifiedComponent<Processor>> newProcessor(final PluginSetting pluginSetting) {
        try {
            final List<Processor> processors = pluginFactory.loadPlugins(
                    Processor.class,
                    pluginSetting,
                    actualClass -> actualClass.isAnnotationPresent(SingleThread.class) ?
                            pluginSetting.getNumberOfProcessWorkers() :
                            1);

            return processors.stream()
                    .map(processor -> new IdentifiedComponent<>(processor, pluginSetting.getName()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            final PluginError pluginError = PluginError.builder()
                    .componentType(PipelineModel.PROCESSOR_PLUGIN_TYPE)
                    .pipelineName(pluginSetting.getPipelineName())
                    .pluginName(pluginSetting.getName())
                    .exception(e)
                    .build();
            pluginErrorCollector.collectPluginError(pluginError);
            return Collections.emptyList();
        }
    }

    private Optional<Source> getSourceIfPipelineType(
            final String sourcePipelineName,
            final PluginSetting pluginSetting,
            final Map<String, Pipeline> pipelineMap,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap) {
        LOG.info("Building [{}] as source component for the pipeline [{}]", pluginSetting.getName(), sourcePipelineName);
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String connectedPipeline = pipelineNameOptional.get();
            if (!sourceConnectorMap.containsKey(sourcePipelineName)) {
                LOG.info("Source of pipeline [{}] requires building of pipeline [{}]", sourcePipelineName,
                        connectedPipeline);
                //Build connected pipeline for the pipeline connector to be available
                //Building like below sometimes yields multiple runs if the pipeline building fails before sink
                //creation. except for running the creation again, it will not harm anything - TODO Fix this
                buildPipelineFromConfiguration(pipelineNameOptional.get(), pipelineConfigurationMap, pipelineMap);
            }
            if (!pipelineMap.containsKey(connectedPipeline)) {
                LOG.error("Connected Pipeline [{}] failed to build, Failing building source for [{}]",
                        connectedPipeline, sourcePipelineName);
                throw new RuntimeException(format("Failed building source for %s, exiting", sourcePipelineName));
            }
            Pipeline sourcePipeline = pipelineMap.get(connectedPipeline);
            final PipelineConnector pipelineConnector = sourceConnectorMap.get(sourcePipelineName);
            pipelineConnector.setSourcePipelineName(pipelineNameOptional.get());
            if (sourcePipeline.getSource().areAcknowledgementsEnabled() || sourcePipeline.getBuffer().areAcknowledgementsEnabled()) {
                pipelineConnector.enableAcknowledgements();
            }
            return Optional.of(pipelineConnector);
        }
        return Optional.empty();
    }

    private DataFlowComponent<Sink> buildRoutedSinkOrConnector(final SinkContextPluginSetting pluginSetting) {
        try {
            final Sink sink = buildSinkOrConnector(pluginSetting, pluginSetting.getSinkContext());

            return new DataFlowComponent<>(sink, pluginSetting.getSinkContext().getRoutes());
        } catch (Exception e) {
            final PluginError pluginError = PluginError.builder()
                    .componentType(PipelineModel.SINK_PLUGIN_TYPE)
                    .pipelineName(pluginSetting.getPipelineName())
                    .pluginName(pluginSetting.getName())
                    .exception(e)
                    .build();
            pluginErrorCollector.collectPluginError(pluginError);
            return null;
        }
    }

    private Sink buildSinkOrConnector(final PluginSetting pluginSetting, final SinkContext sinkContext) {
        LOG.info("Building [{}] as sink component", pluginSetting.getName());
        final Optional<String> pipelineNameOptional = getPipelineNameIfPipelineType(pluginSetting);
        if (pipelineNameOptional.isPresent()) { //update to ifPresentOrElse when using JDK9
            final String pipelineName = pipelineNameOptional.get();
            final PipelineConnector pipelineConnector = new PipelineConnector(pipelineName);
            sourceConnectorMap.put(pipelineName, pipelineConnector); //TODO retrieve from parent Pipeline using name
            return pipelineConnector;
        } else {
            return pluginFactory.loadPlugin(Sink.class, pluginSetting, sinkContext);
        }
    }

    private Optional<String> getPipelineNameIfPipelineType(final PluginSetting pluginSetting) {
        if (PIPELINE_TYPE.equals(pluginSetting.getName()) &&
                pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME) != null) {
            //Validator marked valid config with type as pipeline will have attribute name
            return Optional.of((String) pluginSetting.getAttributeFromSettings(ATTRIBUTE_NAME));
        }
        return Optional.empty();
    }

    /**
     * This removes all built connected pipelines of given pipeline from pipelineMap.
     * TODO Update this to be more elegant and trigger destroy of plugins
     */
    private void removeConnectedPipelines(
            final String failedPipeline,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        final PipelineConfiguration failedPipelineConfiguration = pipelineConfigurationMap.remove(failedPipeline);

        //remove source connected pipelines
        final Optional<String> sourcePipelineOptional = getPipelineNameIfPipelineType(
                failedPipelineConfiguration.getSourcePluginSetting());
        sourcePipelineOptional.ifPresent(sourcePipeline -> processRemoveIfRequired(
                sourcePipeline, pipelineConfigurationMap, pipelineMap));

        //remove sink connected pipelines
        final List<SinkContextPluginSetting> sinkPluginSettings = failedPipelineConfiguration.getSinkPluginSettings();
        sinkPluginSettings.forEach(sinkPluginSetting -> {
            getPipelineNameIfPipelineType(sinkPluginSetting).ifPresent(sinkPipeline -> processRemoveIfRequired(
                    sinkPipeline, pipelineConfigurationMap, pipelineMap));
        });
    }

    private void processRemoveIfRequired(
            final String pipelineName,
            final Map<String, PipelineConfiguration> pipelineConfigurationMap,
            final Map<String, Pipeline> pipelineMap) {
        if (pipelineConfigurationMap.containsKey(pipelineName)) {
            pipelineMap.remove(pipelineName);
            sourceConnectorMap.remove(pipelineName);
            removeConnectedPipelines(pipelineName, pipelineConfigurationMap, pipelineMap);
        }
    }

    private static class IdentifiedComponent<T> {
        private final T component;
        private final String name;

        private IdentifiedComponent(final T component, final String name) {
            this.component = component;
            this.name = name;
        }

        T getComponent() {
            return component;
        }

        String getName() {
            return name;
        }
    }

    Duration getPeerForwarderDrainTimeout(final DataPrepperConfiguration dataPrepperConfiguration) {
        return Optional.ofNullable(dataPrepperConfiguration)
                .map(DataPrepperConfiguration::getPeerForwarderConfiguration)
                .map(PeerForwarderConfiguration::getDrainTimeout)
                .orElse(Duration.ofSeconds(0));
    }

    List<Buffer> getSecondaryBuffers() {
        return peerForwarderProvider.getPipelinePeerForwarderReceiveBufferMap().entrySet().stream()
                .flatMap(entry -> entry.getValue().entrySet().stream())
                .map(innerEntry -> innerEntry.getValue())
                .collect(Collectors.toList());
    }

    private Buffer applyCircuitBreakerToBuffer(final Source source, final Buffer buffer) {
        if (source instanceof PipelineConnector)
            return buffer;

        if(buffer.isWrittenOffHeapOnly())
            return buffer;

        return circuitBreakerManager.getGlobalCircuitBreaker()
                .map(circuitBreaker -> new CircuitBreakingBuffer<>(buffer, circuitBreaker))
                .map(b -> (Buffer) b)
                .orElseGet(() -> buffer);
    }

    public PipelinesDataFlowModel getPipelinesDataFlowModel() {
        return pipelinesDataFlowModel;
    }
}
