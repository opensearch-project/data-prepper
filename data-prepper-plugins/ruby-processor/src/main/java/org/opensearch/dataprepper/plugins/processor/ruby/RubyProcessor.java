package org.opensearch.dataprepper.plugins.processor.ruby;

import org.jruby.RubyInstanceConfig;
import org.jruby.embed.ScriptingContainer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

@DataPrepperPlugin(name= "ruby", pluginType = Processor.class, pluginConfigurationType = RubyProcessorConfig.class)
public class RubyProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(RubyProcessor.class);
    private final RubyProcessorConfig config;
    private final String codeToInject;
    private ScriptingContainer container;
    private String script;

    private Boolean runningCodeFromFile = false;

    private SeekableInputStream fileStream;
    private boolean fileCodeContainsInitCall = false;
    private boolean shouldThrowExceptionSinceNoProcessFound = false;


    @DataPrepperPluginConstructor
    public RubyProcessor(final PluginMetrics pluginMetrics, final RubyProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
        this.codeToInject = config.getCode();

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
            runningCodeFromFile = true;
            runInitCodeFromFileAndDefineProcessMethod();
        }
    }

    private void runFileInitCode(Map<String, String> params) {
        // todo: make params optional?
        container.callMethod("", "init", params);
    }

    private void verifyFileExists(final String codeFilePath) {
        File codeFile = new File(codeFilePath);
        LocalInputFile inputFile = new LocalInputFile(codeFile);

        try {
            this.fileStream = inputFile.newStream();
            // todo: should be able to extend with S3 file input source, as it returns the same SeekableInputStream.
        } catch (Exception exception) {
            LOG.error("Error opening the input file path [{}]", codeFilePath, exception);
            throw new RuntimeException(format("Error opening the input file %s",
                    codeFilePath), exception);
        }

        try {
            SeekableInputStream fileStreamForTestingRegex = inputFile.newStream();
            // need two booleans about
            this.fileCodeContainsInitCall = assertThatRubyProcessorInterfaceImplementedAndReturnInitMethodDefined(
                    fileStreamForTestingRegex);

        } catch (IOException exception) {
            LOG.error("Error panother file or regex exception.", exception);
            throw new RuntimeException(exception.toString());
        }

        if (this.shouldThrowExceptionSinceNoProcessFound) {
            LOG.error("must implement interface described in README when using Ruby code from file. Exiting pipeline.");
            throw new RuntimeException("Ruby Processor file bad implementation");
        }
    }

    private boolean assertThatRubyProcessorInterfaceImplementedAndReturnInitMethodDefined(SeekableInputStream fileStreamForTestingRegex)
    throws IOException
    {
        String rubyCodeAsString = convertInputStreamToString(fileStreamForTestingRegex);

        String processPattern = "def process\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)";

        this.shouldThrowExceptionSinceNoProcessFound = !stringContainsPattern(rubyCodeAsString, processPattern);

        String initPattern = "def init\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\)";
        boolean correctInitMethodFound = stringContainsPattern(rubyCodeAsString, initPattern);

        if (!correctInitMethodFound) {
            String bareboneInitPattern = "def init";
            boolean incorrectInitMethodSignature = stringContainsPattern(rubyCodeAsString, bareboneInitPattern);

            if (incorrectInitMethodSignature) {
                LOG.error("must implement interface described in README when using Ruby code from file " +
                        "— init method signature is incorrect. Exiting pipeline.");
                throw new RuntimeException("Ruby Processor file bad implementation");
            }
        }
        return correctInitMethodFound;
    }

    private boolean stringContainsPattern(String inString, String pattern) {
        Pattern compiledPattern = Pattern.compile(pattern);
        Matcher matcher = compiledPattern.matcher(inString);
        return matcher.find();
    }

    private String convertInputStreamToString(SeekableInputStream inputStream)
    throws IOException
    {
        // from https://stackoverflow.com/questions/309424/how-do-i-read-convert-an-inputstream-into-a-string-in-java,
        // fastest solution.
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int length; (length = inputStream.read(buffer)) != -1; ) {
            result.write(buffer, 0, length);
        }
        return result.toString("UTF-8");
    }

    private void runInitCodeFromFileAndDefineProcessMethod() {
        // Below will also pull in any dependencies in `require` statements.
        container.runScriptlet(this.fileStream, "RUBY_FILE_FOR_LOGGING_PURPOSES");

        if (this.fileCodeContainsInitCall) {
            runFileInitCode(config.getParams());
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final List<Event> events = records.stream().map(Record::getData).collect(Collectors.toList());

        if (runningCodeFromFile) {
            processEventsWithFileCode(events);
        } else {
            injectAndProcessEvents(events);
        }
        return records;
    }

    private void processEventsWithFileCode(final List<Event> events) {
        container.put("events", events);

        script = "events.each { |event| \n"
                + "process(event)\n" +
                "}";
        // todo: make it like LogStash where it returns an array of events?
        container.runScriptlet(script);
    }

    private void injectAndProcessEvents(List<Event> events) {
        container.put("events", events);
        script = "events.each { |event| \n"
                + this.codeToInject +
                "}";

        try {
            container.runScriptlet(script);
        } catch (Exception exception) {
            LOG.error("Exception while processing Ruby code on Events: [{}]",events, exception);
            if (!config.isIgnoreException()) {
                throw new RuntimeException(format("Exception while processing Ruby code on Events: [%s]",events),
                        exception
                );
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

    public String getCodeToExecute() {
        return this.script;
    }
}