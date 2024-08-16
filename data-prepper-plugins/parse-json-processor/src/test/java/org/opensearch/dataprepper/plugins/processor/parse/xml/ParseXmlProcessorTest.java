package org.opensearch.dataprepper.plugins.processor.parse.xml;

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
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.parse.AbstractParseProcessor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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

    private AbstractParseProcessor parseXmlProcessor;
    private final EventFactory testEventFactory = TestEventFactory.getTestEventFactory();
    private final EventKeyFactory testEventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @BeforeEach
    public void setup() {
        when(processorConfig.getSource()).thenReturn(DEFAULT_SOURCE);
        when(processorConfig.getParseWhen()).thenReturn(null);
        when(processorConfig.getOverwriteIfDestinationExists()).thenReturn(true);
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
    }

    @Test
    void test_when_using_invalid_xml_tags_correctly() {

        final String tagOnFailure = UUID.randomUUID().toString();
        when(processorConfig.getTagsOnFailure()).thenReturn(List.of(tagOnFailure));

        parseXmlProcessor = createObjectUnderTest();

        final String serializedMessage = "invalidXml";
        final Event parsedEvent = createAndParseMessageEvent(serializedMessage);

        assertThat(parsedEvent.getMetadata().hasTags(List.of(tagOnFailure)), equalTo(true));
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
