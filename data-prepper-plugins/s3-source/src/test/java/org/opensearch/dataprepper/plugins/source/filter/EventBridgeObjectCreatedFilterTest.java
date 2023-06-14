package org.opensearch.dataprepper.plugins.source.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.parser.ParsedMessage;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventBridgeObjectCreatedFilterTest {
    private EventBridgeObjectCreatedFilter eventBridgeObjectCreatedFilter;
    private ParsedMessage parsedMessage;


    @BeforeEach
    void setUp() {
        eventBridgeObjectCreatedFilter = new EventBridgeObjectCreatedFilter();
        parsedMessage = mock(ParsedMessage.class);
    }

    @Test
    void filter_with_detail_type_Object_Created_should_return_non_empty_instance_of_optional() {
        when(parsedMessage.getDetailType()).thenReturn("Object Created");
        Optional<ParsedMessage> actualValue = eventBridgeObjectCreatedFilter.filter(parsedMessage);

        assertThat(actualValue, instanceOf(Optional.class));
        assertTrue(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.of(parsedMessage)));
    }

    @Test
    void filter_with_eventName_ObjectRemoved_should_return_empty_instance_of_optional() {
        when(parsedMessage.getDetailType()).thenReturn("Object Deleted");
        Optional<ParsedMessage> actualValue = eventBridgeObjectCreatedFilter.filter(parsedMessage);

        assertThat(actualValue, instanceOf(Optional.class));
        assertFalse(actualValue.isPresent());
        assertThat(actualValue, equalTo(Optional.empty()));
    }
}
