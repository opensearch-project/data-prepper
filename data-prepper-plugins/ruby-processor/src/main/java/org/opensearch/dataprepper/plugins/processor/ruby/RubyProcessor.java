package org.opensearch.dataprepper.plugins.processor.ruby;

import org.jruby.RubyInstanceConfig;
import org.jruby.embed.ScriptingContainer;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

@DataPrepperPlugin(name= "ruby", pluginType = Processor.class, pluginConfigurationType = RubyProcessorConfig.class)
public class RubyProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RubyProcessor.class);
    // matches: any method with def [method_name] and a single parameter that is validly named in Ruby.
    private static final String RUBY_METHOD_PATTERN = "def %s\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)";
    static final String RUBY_METHOD_PROCESS_PATTERN = String.format(RUBY_METHOD_PATTERN, "process");
    static final String RUBY_METHOD_INIT_PATTERN = String.format(RUBY_METHOD_PATTERN, "init");
    private static final String PROCESS_METHOD_NOT_FOUND_ERROR_MESSAGE =
            "must implement interface described in README when using Ruby code from file. Exiting pipeline.";
    private static final String BAD_INIT_METHOD_SIGNATURE_ERROR_MESSAGE = "must implement interface described in README" +
            " when using Ruby code from file — init method signature is incorrect. Exiting pipeline.";
    private final RubyProcessorConfig config;
    private final String codeToInject;
    private final ScriptingContainer container;
    private String script;
    private InputStream fileStream;
    private boolean fileCodeContainsInitCall = false;

    @DataPrepperPluginConstructor
    public RubyProcessor(final PluginMetrics pluginMetrics, final RubyProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
        this.codeToInject = config.isCodeFromFile() ? "process(event)" : config.getCode();

        container = new ScriptingContainer();
        container.setCompileMode(RubyInstanceConfig.CompileMode.JIT);

        container.put("LOG", LOG); // inject logger, perform cold start

        if (config.isInitDefined()) {
            container.put("params", config.getParams());
            container.runScriptlet(config.getInitCode());
            container.remove("params");
        }

        if (config.isCodeFromFile()) {
            verifyFileExists(config.getPath());
            runInitCodeFromFileAndDeclareTheProcessMethod();
        }
    }

    private void verifyFileExists(final String codeFilePath) {
        File codeFile = new File(codeFilePath);
        LOG.debug("Attempting to access .rb file with absolute path {}", codeFile.getAbsolutePath());
        final LocalInputFile inputFile = new LocalInputFile(codeFile);

        try {
            this.fileStream = inputFile.newStream();
            // todo: should be able to extend with S3 file input source, as it returns the same InputStream.
        } catch (Exception exception) {
            LOG.error("Error opening the input file path [{}]", codeFilePath, exception);
            throw new RuntimeException(format("Error opening the input file %s",
                    codeFilePath), exception);
        }

        try {
            // create new InputStream since the stream will be consumed by assertThatInterfaceImplementedInRubyFile
            final InputStream fileStreamForTestingRegex = inputFile.newStream();

            assertThatInterfaceImplementedInRubyFile(fileStreamForTestingRegex);

        } catch (IOException exception) {
            LOG.error("Error: file or regex exception " +
                    "while starting up Ruby processor with code from file.", exception);
            throw new RuntimeException(exception.toString());
        }
    }

    private void assertThatInterfaceImplementedInRubyFile(final InputStream fileStreamForTestingRegex)
    throws IOException
    {
        final String rubyCodeAsString = convertInputStreamToString(fileStreamForTestingRegex);

        if (!stringContainsPattern(rubyCodeAsString, RUBY_METHOD_PROCESS_PATTERN)) {
            throwBadInterfaceImplementationException(PROCESS_METHOD_NOT_FOUND_ERROR_MESSAGE);
        }

        this.fileCodeContainsInitCall = stringContainsPattern(rubyCodeAsString,
                RUBY_METHOD_INIT_PATTERN);

        if (!this.fileCodeContainsInitCall) {
            String bareboneInitPattern = "def init";
            boolean incorrectInitMethodSignature = stringContainsPattern(rubyCodeAsString, bareboneInitPattern);

            if (incorrectInitMethodSignature) {
                throwBadInterfaceImplementationException(BAD_INIT_METHOD_SIGNATURE_ERROR_MESSAGE);
            }
        }
    }

    private void throwBadInterfaceImplementationException(final String errorMessage) {
        LOG.error(errorMessage);
        throw new RuntimeException("Ruby Processor file bad implementation");
    }

    private boolean stringContainsPattern(final String inString, final String pattern) {
        final Pattern compiledPattern = Pattern.compile(pattern);
        final Matcher matcher = compiledPattern.matcher(inString);
        return matcher.find();
    }

    private String convertInputStreamToString(final InputStream inputStream)
    throws IOException
    {
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private void runInitCodeFromFileAndDeclareTheProcessMethod() {
        // Below will also pull in any dependencies in `require` statements.
        container.runScriptlet(this.fileStream, "RUBY_FILE_FOR_LOGGING_PURPOSES");

        if (this.fileCodeContainsInitCall) {
            container.callMethod("", "init", config.getParams());
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final List<Event> events = records.stream().map(Record::getData).collect(Collectors.toList());
        injectAndProcessEvents(events);

        return records;
    }

    private void injectAndProcessEvents(final List<Event> events) {
        container.put("events", events);
        script = "events.each { |event| \n"
                + this.codeToInject +
                "}";

        try {
            container.runScriptlet(script);
        } catch (Exception exception) {
            LOG.error(DataPrepperMarkers.SENSITIVE, "Exception while processing Event batch in Ruby code. " +
                    "Retrying Events one by one.", exception);
            retryEventsOneByOneAndApplyTags(events);
        }
    }

    private void retryEventsOneByOneAndApplyTags(final List<Event> events) {
        if (config.getTagsOnFailure().isEmpty()) {
            LOG.debug("No tags to apply to Event that triggers Ruby processor exception. The exception and the Event " +
                    "will be logged as an Error.");
        }
        for (Event event : events) {
            container.put("event", event);
            try {
                container.runScriptlet(this.codeToInject);
            } catch (Exception exception) {
                LOG.error(DataPrepperMarkers.SENSITIVE, "Exception within Ruby processor on Event: [{}]",
                        event, exception);
                event.getMetadata().addTags(config.getTagsOnFailure());
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