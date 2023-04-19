package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DocumentBuilderTest {

    private String random;
    private Event event;
    private String expectedOutput;

    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setup() throws JsonProcessingException {
        final String random = UUID.randomUUID().toString();
        Map<String, Object> nestedData = Map.of("random", random, "triangle", "equilateral");
        Map<String, Object> data = Map.of("foo", 42, "nested", nestedData, "boolean", false);

        event = JacksonEvent.builder()
            .withData(data)
            .withEventType("TestEvent")
            .build();
        expectedOutput = objectMapper.writeValueAsString(data);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"missingObject", "/"})
    public void buildWillReturnFullObject(final String documentRootKey) {

        final String doc = DocumentBuilder.build(event, documentRootKey);

        assertThat(doc, is(equalTo(expectedOutput)));
    }

    @ParameterizedTest
    @MethodSource("provideSingleItemKeys")
    public void buildWillReturnSingleObject(final String documentRootKey, final Object expectedResult) {

        final String doc = DocumentBuilder.build(event, documentRootKey);

        assertThat(doc, is(equalTo(String.format("{\"data\": %s}", expectedResult))));
    }

    private static Stream<Arguments> provideSingleItemKeys() {
        return Stream.of(
            Arguments.of("foo", 42),
            Arguments.of("boolean", false),
            Arguments.of("nested/triangle", "\"equilateral\"")
        );
    }
}
