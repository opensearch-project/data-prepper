package org.opensearch.dataprepper.plugins.processor.ruby;

import org.jruby.RubyInstanceConfig;
import org.jruby.embed.EvalFailedException;
import org.jruby.embed.ScriptingContainer;
import org.jruby.exceptions.Exception;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;


@DataPrepperPlugin(name= "ruby", pluginType = Processor.class, pluginConfigurationType = RubyProcessorConfig.class)
public class RubyProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RubyProcessor.class);
    private final RubyProcessorConfig config;
    private final String codeToInject;

    private ScriptingContainer container;
    
    @DataPrepperPluginConstructor
    public RubyProcessor(final PluginMetrics pluginMetrics, final RubyProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
        this.codeToInject = config.getCode();

        container = new ScriptingContainer();
        container.setCompileMode(RubyInstanceConfig.CompileMode.JIT);

        container.put("LOG", LOG); // inject logger, perform cold start

        if (Objects.nonNull(config.getInitCode())) {
            container.runScriptlet(config.getInitCode());
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final List<Event> events = records.stream().map(Record::getData).collect(Collectors.toList());

        injectAndProcessEvents(events);

        return records;
    }

    private void injectAndProcessEvents(List<Event> events) {
        container.put("events", events);
        String script = "events.each { |event| \n"
                + this.codeToInject +
                "}";

        try {
            container.runScriptlet(script);
        } catch (EvalFailedException exception) {
            LOG.error("Exception processing Ruby code", exception);
            if (!config.isIgnoreException()) {
                throw new RuntimeException(); // todo: will this break the pipeline, as intended?
            }
        }
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
        container.terminate();
    }
}