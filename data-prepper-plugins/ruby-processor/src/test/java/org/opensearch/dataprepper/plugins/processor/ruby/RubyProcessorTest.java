package org.opensearch.dataprepper.plugins.processor.ruby;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RubyProcessorTest {
    @Mock
    private RubyProcessorConfig processorConfig;
    @Mock
    private PluginMetrics pluginMetrics;

    private static final String RUBY_NOOP_CODE = "nil";
    private static final String RUBY_EXCEPTION_CODE = "raise StandardError, 'Error from within Ruby'";

    private RubyProcessor rubyProcessor;

    @BeforeEach
    void setup() {
        RubyProcessorConfig defaultConfig = new RubyProcessorConfig();

        rubyProcessor = createObjectUnderTest();
    }

    private RubyProcessor createObjectUnderTest() {
        return new RubyProcessor(pluginMetrics, processorConfig);
    }

    @Test
    void test_when_noopCodeSpecifiedThenEventsAreUnmodified() {
        when(processorConfig.getCode()).thenReturn(RUBY_NOOP_CODE);
        rubyProcessor = createObjectUnderTest(); // to get updated code.

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","message datum.");

        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> editedEvents = (List<Record<Event>>) rubyProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThatEventsAreFunctionallyIdentical(eventUnderTest.getData(), parsedEvent);
    }

    @Test
    void test_when_exceptionInRubyThenDoesNotCrashPipeline() {
        when(processorConfig.getCode()).thenReturn(RUBY_EXCEPTION_CODE);
        rubyProcessor = createObjectUnderTest(); // to get updated code.
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","message datum.");

        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> parsedRecords = (List<Record<Event>>) rubyProcessor.doExecute(
                Collections.singletonList(eventUnderTest));

        final Event parsedEvent = parsedRecords.get(0).getData();
        assertThat(parsedEvent.get("message", String.class), equalTo("message datum."));
    }

    @Test
    void test_when_exceptionInRubyAndIgnoreExceptionSpecifiedThenPipelineDoesNotCrash() {
        // todo: is the intended behavior to persist this given event, or something else?
        when(processorConfig.getCode()).thenReturn(RUBY_EXCEPTION_CODE);
        rubyProcessor = createObjectUnderTest(); // to get updated code.

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","message datum.");

        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> editedEvents = (List<Record<Event>>) rubyProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThatEventsAreFunctionallyIdentical(eventUnderTest.getData(), parsedEvent);
    }


    private void assertThatEventsAreFunctionallyIdentical(Event sourceEvent, Event targetEvent) {
        assertThat(sourceEvent.toJsonString(), equalTo(targetEvent.toJsonString()));
        assertThat(sourceEvent.getMetadata(), equalTo(targetEvent.getMetadata()));
    }

    private Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Event getSingleEvent(final List<Record<Event>> editedRecords) {
        return editedRecords.get(0).getData();
    }

}
