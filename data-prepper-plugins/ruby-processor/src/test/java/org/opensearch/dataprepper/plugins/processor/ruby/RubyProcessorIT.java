package org.opensearch.dataprepper.plugins.processor.ruby;

import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.loggenerator.ApacheLogFaker;
import org.opensearch.dataprepper.plugins.source.loggenerator.LogGeneratorSource;
import org.opensearch.dataprepper.plugins.source.loggenerator.logtypes.CommonApacheLogTypeGenerator;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

public class RubyProcessorIT {
    private static final int NUMBER_EVENTS_TO_TEST = 100;
    private static final String PLUGIN_NAME = "ruby";
    private static final String TEST_PIPELINE_NAME = "ruby_processor_test";

    static String CODE_STRING = "require 'java'\n" + "puts event.class\n" +
            "event.put('downcase', event.get('message', Object.class).downcase)";

    private RubyProcessor rubyProcessor;

    private RubyProcessorConfig rubyProcessorConfig;
    private CommonApacheLogTypeGenerator apacheLogGenerator;

    @BeforeEach
    void setup() {
        rubyProcessorConfig = new RubyProcessorConfig();
        try {
            setField(RubyProcessorConfig.class, RubyProcessorIT.this.rubyProcessorConfig, "code", CODE_STRING);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        apacheLogGenerator = new CommonApacheLogTypeGenerator();

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        rubyProcessor = new RubyProcessor( pluginMetrics, rubyProcessorConfig);
    }

    @Test
    void when_eventMessageIsDowncasedByRubyProcessor_then_eventMessageIsDowncased() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_standardRubyLibraryIsImported_then_noErrorsAndEventsCanBeModified() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());
        CODE_STRING = "require 'strscan'\n" + // todo: modify with this in the init section.
                "s = StringScanner.new(event.get('message'.to_java, java.lang.String.java_class))\n" +
                "event.put('emptyString', s.eos?)"; // True iff message is eos;
        setup();
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("emptyString", Boolean.class), equalTo(false));
            // True iff message is eos, which is impossible as Apache logs are not empty.
        }
    }

    @Test
    void when_needToCleanScriptAPICallsAndJavaAlreadyRequired_then_works() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "require 'java'\n" + "puts event.class\n" +
                "event.put('downcase', event.get('message', Object.class).downcase)";
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_needToCleanScriptAPICallsAndJavaNotRequired_then_injectsJavaAndWorks() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "event.put('downcase', event.get('message', Object.class).downcase)";
        setup();

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        for (int recordNumber = 0; recordNumber < records.size(); recordNumber++) {
            final Event parsedEvent = parsedRecords.get(recordNumber).getData();
            final String originalString = parsedEvent.get("message", String.class);
            assertThat(parsedEvent.get("downcase", String.class), equalTo(originalString.toLowerCase()));
        }
    }

    @Test
    void when_dotClassMethodCalledAnywhereBesidesEventGetCall_then_remainsUnchanged() {
        final List<Record<Event>> records = IntStream.range(0, NUMBER_EVENTS_TO_TEST)
                .mapToObj(i -> new Record<>(apacheLogGenerator.generateEvent()))
                .collect(Collectors.toList());

        CODE_STRING = "puts event.class\n" +
                "event.put('downcase', event.get('message', Object.class).downcase)";

        setup();
        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(records);

        assertThat(rubyProcessor.getCodeToExecute().contains("puts event.class"), equalTo(true));
    // todo: will require a refactor to assert
    }
}
