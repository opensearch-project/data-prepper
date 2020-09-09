package com.amazon.situp;

import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.sink.Sink;
import com.amazon.situp.model.source.Source;
import com.amazon.situp.parser.PipelineParser;
import com.amazon.situp.parser.model.PipelineConfiguration;
import com.amazon.situp.pipeline.Pipeline;
import com.amazon.situp.plugins.buffer.BufferFactory;
import com.amazon.situp.plugins.processor.ProcessorFactory;
import com.amazon.situp.plugins.sink.SinkFactory;
import com.amazon.situp.plugins.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SITUP is the entry point into the execution engine. An instance of this class is provided by
 * {@link #getInstance()} method and the same can eb used to trigger execution via {@link #execute()} of the
 * {@link Pipeline} with default configuration or {@link #execute(String)} to provide custom configuration file. Also,
 * the same instance reference can be further used to {@link #stop()} the execution.
 */
public class Situp {
    private static final Logger LOG = LoggerFactory.getLogger(Situp.class);

    private static final String DEFAULT_CONFIG_LOCATION = "situp-core/src/main/resources/situp-default.yml";
    private static final String PROCESSOR_THREADS_ATTRIBUTE = "threads";
    private Pipeline transformationPipeline;

    private static volatile Situp situp;

    public static Situp getInstance() {
        if (situp == null) {
            synchronized (Situp.class) {
                if (situp == null)
                    situp = new Situp();
            }
        }
        return situp;
    }

    private Situp() {
        if (situp != null) {
            throw new RuntimeException("Please use getInstance() for an instance of this SITUP");
        }
    }

    /**
     * Executes SITUP engine using the default configuration file/
     *
     * @return true if the execute successfully initiates the SITUP
     */
    public boolean execute() {
        return execute(DEFAULT_CONFIG_LOCATION);
    }

    /**
     * Executes SITUP engine using the default configuration file/
     *
     * @param configurationFileLocation the location of the configuration file
     * @return true if the execute successfully initiates the SITUP
     */
    public boolean execute(final String configurationFileLocation) {
        LOG.info("Using {} configuration file",configurationFileLocation);
        final PipelineParser pipelineParser = new PipelineParser(configurationFileLocation);
        final PipelineConfiguration pipelineConfiguration = pipelineParser.parseConfiguration();
        execute(pipelineConfiguration);
        return true;
    }

    /**
     * Terminates the execution of SITUP.
     * TODO return boolean status of the stop request
     */
    public void stop() {
        transformationPipeline.stop();
    }

    /**
     * Executes SITUP engine for the provided {@link PipelineConfiguration}.
     * @param pipelineConfiguration to be used for {@link Pipeline} execution
     * @return true if the execute successfully initiates the SITUP
     * {@link com.amazon.situp.pipeline.Pipeline} execute.
     */
    private boolean execute(final PipelineConfiguration pipelineConfiguration) {
        transformationPipeline = buildPipelineFromConfiguration(pipelineConfiguration);
        LOG.info("Successfully parsed the configuration file, Triggering pipeline execution");
        transformationPipeline.execute();
        return true;
    }

    @SuppressWarnings({"rawtypes"})
    private Pipeline buildPipelineFromConfiguration(final PipelineConfiguration pipelineConfiguration) {
        final Source source = SourceFactory.newSource(getFirstSettingsIfExists(pipelineConfiguration.getSource()));
        final PluginSetting bufferPluginSetting = getFirstSettingsIfExists(pipelineConfiguration.getBuffer());
        final Buffer buffer = bufferPluginSetting == null ? null : BufferFactory.newBuffer(bufferPluginSetting);

        final Configuration processorConfiguration = pipelineConfiguration.getProcessor();
        final List<PluginSetting> processorPluginSettings = processorConfiguration.getPluginSettings();
        final List<Processor> processors = processorPluginSettings.stream()
                .map(ProcessorFactory::newProcessor)
                .collect(Collectors.toList());

        final List<PluginSetting> sinkPluginSettings = pipelineConfiguration.getSink().getPluginSettings();
        final List<Sink> sinks = sinkPluginSettings.stream().map(SinkFactory::newSink).collect(Collectors.toList());

        final int processorThreads = getConfiguredThreadsOrDefault(processorConfiguration);

        return new Pipeline(pipelineConfiguration.getName(), source, buffer, processors, sinks, processorThreads);
    }

    private PluginSetting getFirstSettingsIfExists(final Configuration configuration) {
        final List<PluginSetting> pluginSettings = configuration.getPluginSettings();
        return pluginSettings.isEmpty() ? null : pluginSettings.get(0);
    }

    private int getConfiguredThreadsOrDefault(final Configuration processorConfiguration) {
        int processorThreads = processorConfiguration.getAttributeValueAsInteger(PROCESSOR_THREADS_ATTRIBUTE);
        return processorThreads <= 0 ? getDefaultProcessorThreads() : processorThreads;
    }

    /**
     * TODO Implement this to use CPU cores of the executing machine
     */
    private int getDefaultProcessorThreads() {
        return 1;
    }

}