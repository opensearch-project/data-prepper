package com.amazon.situp.parser;

import com.amazon.situp.parser.model.PipelineConfiguration;
import org.apache.bval.jsr.ApacheValidationProvider;
import org.junit.AfterClass;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.util.Set;

import static com.amazon.situp.TestDataProvider.INVALID_BUFFER_VIOLATION_MESSAGE;
import static com.amazon.situp.TestDataProvider.INVALID_PROCESSOR_VIOLATION_MESSAGE;
import static com.amazon.situp.TestDataProvider.INVALID_SINK_VIOLATION_MESSAGE;
import static com.amazon.situp.TestDataProvider.INVALID_SOURCE_VIOLATION_MESSAGE;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithAllInvalidPlugins;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithBufferButEmptyName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithBufferButNullName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleBuffers;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleProcessors;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleProcessorsSomeInvalid;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleSinks;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleSinksSomeInvalid;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithMultipleSources;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNoBuffer;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNoPluginsForSource;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNoProcessors;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithNoSinks;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithProcessorsButEmptyName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithProcessorsButNullName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithSinkButEmptyName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithSinkButNullName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithSourceButEmptyName;
import static com.amazon.situp.TestDataProvider.pipelineConfigurationWithSourceButNullName;
import static com.amazon.situp.TestDataProvider.validPipelineConfiguration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

public class PipelineConfigurationTests {
    public static ValidatorFactory JSR_VALIDATOR_FACTORY = Validation.byProvider(ApacheValidationProvider.class)
            .configure().buildValidatorFactory();
    public static Validator JSR_VALIDATOR = JSR_VALIDATOR_FACTORY.getValidator();

    @AfterClass
    public static void tearDown() {
        JSR_VALIDATOR_FACTORY.close();
    }

    @Test
    public void testPipelineConfigurationWithValidSource() {
        final PipelineConfiguration pipelineConfiguration = validPipelineConfiguration();
        executeValidationAndAssert(pipelineConfiguration, 0, "");
    }

    @Test
    public void testPipelineConfigurationWithSourceButEmptyName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithSourceButEmptyName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SOURCE_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithSourceButNullName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithSourceButNullName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SOURCE_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithNoPluginsForSource() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithNoPluginsForSource();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SOURCE_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithMultipleSources() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleSources();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SOURCE_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithNoBuffers() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithNoBuffer();
        executeValidationAndAssert(pipelineConfiguration, 0, "");
    }

    @Test
    public void testPipelineConfigurationWithBufferButEmptyName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithBufferButEmptyName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_BUFFER_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithBufferButNullName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithBufferButNullName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_BUFFER_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithMultipleBuffers() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleBuffers();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_BUFFER_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithMultipleProcessors() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleProcessors();
        executeValidationAndAssert(pipelineConfiguration, 0, "");
    }

    @Test
    public void testPipelineConfigurationWithNoProcessors() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithNoProcessors();
        executeValidationAndAssert(pipelineConfiguration, 0, "");
    }

    @Test
    public void testPipelineConfigurationWithMultipleProcessorsSomeInvalid() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleProcessorsSomeInvalid();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_PROCESSOR_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithProcessorsButEmptyName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithProcessorsButEmptyName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_PROCESSOR_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithProcessorsButNullName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithProcessorsButNullName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_PROCESSOR_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithMultipleSinks() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleSinks();
        executeValidationAndAssert(pipelineConfiguration, 0, "");
    }

    @Test
    public void testPipelineConfigurationWithSinksButEmptyName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithSinkButEmptyName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SINK_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithSinksButNullName() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithSinkButNullName();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SINK_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithMultipleSinksSomeInvalid() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithMultipleSinksSomeInvalid();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SINK_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithNoSinks() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithNoSinks();
        executeValidationAndAssert(pipelineConfiguration, 1, INVALID_SINK_VIOLATION_MESSAGE);
    }

    @Test
    public void testPipelineConfigurationWithAllInvalidPlugins() {
        final PipelineConfiguration pipelineConfiguration = pipelineConfigurationWithAllInvalidPlugins();
        final Set<ConstraintViolation<PipelineConfiguration>> violations = JSR_VALIDATOR.validate(pipelineConfiguration);
        assertThat(violations, iterableWithSize(4));
    }

    private void executeValidationAndAssert(final PipelineConfiguration pipelineConfiguration,
                                            int expectedViolationCount,
                                            final String expectedMessage) {
        final Set<ConstraintViolation<PipelineConfiguration>> violations = JSR_VALIDATOR.validate(pipelineConfiguration);
        assertThat(violations, iterableWithSize(expectedViolationCount));
        if (expectedViolationCount != 0) {
            for (ConstraintViolation<PipelineConfiguration> violation : violations) {
                assertThat(violation.getMessage(), is(expectedMessage));
            }
        }
    }

}