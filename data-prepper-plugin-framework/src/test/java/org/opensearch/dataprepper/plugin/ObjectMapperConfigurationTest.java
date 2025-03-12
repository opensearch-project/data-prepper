/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDeserializationProblemHandler;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ObjectMapperConfigurationTest {
    private final ObjectMapperConfiguration objectMapperConfiguration = new ObjectMapperConfiguration();

    @Mock
    private VariableExpander variableExpander;

    @Mock
    private EventKeyFactory eventKeyFactory;

    @Mock
    private DataPrepperDeserializationProblemHandler dataPrepperDeserializationProblemHandler;

    @Test
    void test_duration_with_pluginConfigObjectMapper() {
        final String durationTestString = "10s";
        final ObjectMapper objectMapper = objectMapperConfiguration.pluginConfigObjectMapper(
                variableExpander, eventKeyFactory, dataPrepperDeserializationProblemHandler);
        final Duration duration = objectMapper.convertValue(durationTestString, Duration.class);
        assertThat(duration, equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void test_enum_with_pluginConfigObjectMapper() throws JsonProcessingException {
        final String testModelAsString = "{ \"name\": \"my-name\", \"test_type\": \"test\" }";
        final ObjectMapper objectMapper = objectMapperConfiguration.pluginConfigObjectMapper(
                variableExpander, eventKeyFactory, dataPrepperDeserializationProblemHandler);
        final TestModel testModel = objectMapper.readValue(testModelAsString, TestModel.class);
        assertThat(testModel, notNullValue());
        assertThat(testModel.getTestType(), equalTo(TestType.TEST));
    }

    @Test
    void test_duration_with_extensionPluginConfigObjectMapper() {
        final String durationTestString = "10s";
        final ObjectMapper objectMapper = objectMapperConfiguration.extensionPluginConfigObjectMapper(
                dataPrepperDeserializationProblemHandler);
        final Duration duration = objectMapper.convertValue(durationTestString, Duration.class);
        assertThat(duration, equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void test_enum_with_extensionPluginConfigObjectMapper() throws JsonProcessingException {
        final String testModelAsString = "{ \"name\": \"my-name\", \"test_type\": \"test\" }";
        final ObjectMapper objectMapper = objectMapperConfiguration.extensionPluginConfigObjectMapper(
                dataPrepperDeserializationProblemHandler);
        final TestModel testModel = objectMapper.readValue(testModelAsString, TestModel.class);
        assertThat(testModel, notNullValue());
        assertThat(testModel.getTestType(), equalTo(TestType.TEST));
    }

    @Test
    void test_eventKey_with_pluginConfigObjectMapper() {
        final String testKey = "test";
        final EventKey eventKey = mock(EventKey.class);
        when(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.ALL)).thenReturn(eventKey);
        final ObjectMapper objectMapper = objectMapperConfiguration.pluginConfigObjectMapper(
                variableExpander, eventKeyFactory, dataPrepperDeserializationProblemHandler);
        final EventKey actualEventKey = objectMapper.convertValue(testKey, EventKey.class);
        assertThat(actualEventKey, equalTo(eventKey));
    }

    private enum TestType {

        TEST("test");

        private static final Map<String, TestType> NAMES_MAP = Arrays.stream(TestType.values())
                .collect(Collectors.toMap(TestType::toString, Function.identity()));

        private final String name;

        TestType(final String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }

        @JsonCreator
        static TestType fromOptionValue(final String option) {
            return NAMES_MAP.get(option);
        }
    }

    private static class TestModel {
        @JsonProperty("name")
        private final String name;

        @JsonProperty("test_type")
        private final TestType testType;

        public TestModel(@JsonProperty("name") final String name,
                         @JsonProperty("test_type") final TestType testType) {
            this.name = name;
            this.testType = testType;
        }

        public String getName() {
            return name;
        }

        public TestType getTestType() {
            return testType;
        }
    }

}