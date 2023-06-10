package org.opensearch.dataprepper.plugins.processor.ruby;

import org.jruby.RubyInstanceConfig;
import org.jruby.embed.EvalFailedException;
import org.jruby.embed.ScriptingContainer;
import org.jruby.exceptions.NameError;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@DataPrepperPlugin(name= "ruby", pluginType = Processor.class, pluginConfigurationType = RubyProcessorConfig.class)
public class RubyProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RubyProcessor.class);
    private static final CharSequence JACKSON_LOG_CLASS_NAME = "Java::OrgOpensearchDataprepperModelLog::JacksonLog";
    private static final CharSequence JACKSON_EVENT_CLASS_NAME = "Java::OrgOpensearchDataprepperModelEvent::JacksonEvent";
    private static final String REQUIRES_JAVA_STRING = "require 'java'";

    private final RubyProcessorConfig config;
    private final String codeToInject;

    private ScriptingContainer container;
    private String script;

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
        script = "events.each { |event| \n"
                + this.codeToInject +
                "}";

        try {
            container.runScriptlet(script);
        } catch (EvalFailedException exception) {
            LOG.error("Exception processing Ruby code", exception);

            if (exceptionIsNameError(exception) && nameErrorIsEvent(exception.getMessage()))
            {
                LOG.debug("NameError on JacksonEvent caught, trying to recover via overloaded Event");

                container.put("events", events);
                container.runScriptlet(cleanEventAPICalls(script));
            }
            else {
                if (!config.isIgnoreException()) {
                    throw new RuntimeException(); // todo: should we tag instead of breaking pipeline?
                }
            }
        }
    }

    private String cleanEventAPICalls(String script) {
        // 1. inject require('java')
        // 2. change event.get("string", <X>.class) to event.get("<field_name>", java.lang.Object.java_class).
        // everything in (J)Ruby is an object, so this should be safe. Ruby is not statically typed so having the return
        // type be Object is not a huge issue from my perspective.

        Pattern pattern = Pattern.compile("(event\\.get\\(\'.*?\', )(.*?\\.class)");

        Matcher matcher = pattern.matcher(script);
        String replacement = "$1java.lang.Object.java_class";
        String output;

        if (!script.contains(REQUIRES_JAVA_STRING)) {
            output = REQUIRES_JAVA_STRING + "\n" + matcher.replaceAll(replacement);
        } else {
            output = matcher.replaceAll(replacement);
        }

        this.script = output; 
        return output;
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

    public String getCodeToExecute() {
        return this.script;
    }

    private boolean exceptionIsNameError(EvalFailedException exception) {
        return exception.getCause().getClass().equals(NameError.class);
    }

    private boolean nameErrorIsEvent(String message) {
        return message.contains(JACKSON_LOG_CLASS_NAME) || message.contains(JACKSON_EVENT_CLASS_NAME);
    }
}