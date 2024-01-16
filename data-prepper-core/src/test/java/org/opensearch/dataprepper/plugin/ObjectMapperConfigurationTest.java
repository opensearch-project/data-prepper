package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
class ObjectMapperConfigurationTest {
    private final ObjectMapperConfiguration objectMapperConfiguration = new ObjectMapperConfiguration();

    @Mock
    private VariableExpander variableExpander;

    @Test
    void test_duration_with_pluginConfigObjectMapper() {
        final String durationTestString = "10s";
        final ObjectMapper objectMapper = objectMapperConfiguration.pluginConfigObjectMapper(variableExpander);
        final Duration duration = objectMapper.convertValue(durationTestString, Duration.class);
        assertThat(duration, equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void test_enum_with_pluginConfigObjectMapper() {
        final String testString = "test";
        final ObjectMapper objectMapper = objectMapperConfiguration.pluginConfigObjectMapper(variableExpander);
        final TestType duration = objectMapper.convertValue(testString, TestType.class);
        assertThat(duration, equalTo(TestType.fromOptionValue(testString)));
    }

    @Test
    void test_duration_with_extensionPluginConfigObjectMapper() {
        final String durationTestString = "10s";
        final ObjectMapper objectMapper = objectMapperConfiguration.extensionPluginConfigObjectMapper();
        final Duration duration = objectMapper.convertValue(durationTestString, Duration.class);
        assertThat(duration, equalTo(Duration.ofSeconds(10)));
    }

    @Test
    void test_enum_with_extensionPluginConfigObjectMapper() {
        final String testString = "test";
        final ObjectMapper objectMapper = objectMapperConfiguration.extensionPluginConfigObjectMapper();
        final TestType duration = objectMapper.convertValue(testString, TestType.class);
        assertThat(duration, equalTo(TestType.fromOptionValue(testString)));
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

}