package org.opensearch.dataprepper.plugins.processor.parse.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.processor.parse.xml.ParseXmlProcessorConfig.DEFAULT_SOURCE;


@ExtendWith(MockitoExtension.class)
public class ParseXmlProcessorTest {

    @Mock
    private ParseXmlProcessorConfig processorConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Counter processingFailuresCounter;

    @Mock
    private Counter parseErrorsCounter;

    @Mock
    private HandleFailedEventsOption handleFailedEventsOption;

    private AbstractParseProcessor parseXmlProcessor;
    private final EventFactory testEventFactory = TestEventFactory.getTestEventFactory();
    private final EventKeyFactory testEventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @BeforeEach
    public void setup() {
        when(processorConfig.getSource()).thenReturn(DEFAULT_SOURCE);
        when(processorConfig.getParseWhen()).thenReturn(null);
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(true);
        when(pluginMetrics.counter("recordsIn")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("recordsOut")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("processingFailures")).thenReturn(processingFailuresCounter);
        when(pluginMetrics.counter("parseErrors")).thenReturn(parseErrorsCounter);
        when(processorConfig.getHandleFailedEventsOption()).thenReturn(handleFailedEventsOption);
    }

    protected AbstractParseProcessor createObjectUnderTest() {
        return new ParseXmlProcessor(pluginMetrics, processorConfig, expressionEvaluator, testEventKeyFactory);
    }

    @Test
    void test_when_using_xml_features_then_processorParsesCorrectly() {
        parseXmlProcessor = createObjectUnderTest();

        final String serializedMessage = "<Person><name>John Doe</name><age>30</age></Person>";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.get("name", String.class), equalTo("John Doe"));
        assertThat(parsedEvent.get("age", String.class), equalTo("30"));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_deleteSourceFlagEnabled() {

        final String tagOnFailure = UUID.randomUUID().toString();
        when(processorConfig.getTagsOnFailure()).thenReturn(List.of(tagOnFailure));
        when(processorConfig.isDeleteSourceRequested()).thenReturn(true);

        parseXmlProcessor = createObjectUnderTest();

        final String serializedMessage = "<Person><name>John Doe</name><age>30</age></Person>";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);
        assertThat(parsedEvent.containsKey(processorConfig.getSource()), equalTo(false));
        assertThat(parsedEvent.get("name", String.class), equalTo("John Doe"));
        assertThat(parsedEvent.get("age", String.class), equalTo("30"));

        verifyNoInteractions(processingFailuresCounter);
        verifyNoInteractions(handleFailedEventsOption);
    }

    @Test
    void test_when_using_invalid_xml_tags_correctly() {

        final String tagOnFailure = UUID.randomUUID().toString();
        when(processorConfig.getTagsOnFailure()).thenReturn(List.of(tagOnFailure));
        when(handleFailedEventsOption.shouldLog()).thenReturn(true);

        parseXmlProcessor = createObjectUnderTest();

        final String serializedMessage = "invalidXml";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.getMetadata().hasTags(List.of(tagOnFailure)), equalTo(true));

        verify(parseErrorsCounter).increment();
        verifyNoInteractions(processingFailuresCounter);
    }

    @Test
    void test_when_object_mapper_throws_other_exception_tags_correctly() throws JsonProcessingException, NoSuchFieldException, IllegalAccessException {

        final String tagOnFailure = UUID.randomUUID().toString();
        when(processorConfig.getTagsOnFailure()).thenReturn(List.of(tagOnFailure));
        when(handleFailedEventsOption.shouldLog()).thenReturn(true);

        parseXmlProcessor = createObjectUnderTest();

        final XmlMapper mockMapper = mock(XmlMapper.class);
        when(mockMapper.readValue(anyString(), any(TypeReference.class))).thenThrow(IllegalArgumentException.class);

        ReflectivelySetField.setField(ParseXmlProcessor.class, parseXmlProcessor, "xmlMapper", mockMapper);

        final String serializedMessage = "invalidXml";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.getMetadata().hasTags(List.of(tagOnFailure)), equalTo(true));

        verify(processingFailuresCounter).increment();
    }

    private Event createAndParseMessageEvent(final String message) {
        final Record<Event> eventUnderTest = createMessageEvent(message);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) parseXmlProcessor.doExecute(
                Collections.singletonList(eventUnderTest));
        return editedEvents.get(0).getData();
    }

    private Record<Event> createMessageEvent(final String message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put(processorConfig.getSource(), message);
        return buildRecordWithEvent(eventData);
    }

    private Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(testEventFactory.eventBuilder(EventBuilder.class).withData(data).build());
    }
}
