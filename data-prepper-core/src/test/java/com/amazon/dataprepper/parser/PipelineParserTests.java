package com.amazon.dataprepper.parser;

import com.amazon.dataprepper.pipeline.Pipeline;
import org.junit.Test;

import java.util.Map;

import static com.amazon.dataprepper.TestDataProvider.CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_PROCESSORS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_MULTIPLE_SINKS_CONFIG_FILE;
import static com.amazon.dataprepper.TestDataProvider.VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineParserTests {

    @Test
    public void testValidMultiplePipelines() {
        final PipelineParser pipelineParser = new PipelineParser(VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final Map<String, Pipeline> actualPipelineMap = pipelineParser.parseConfiguration();
        assertThat(actualPipelineMap.keySet(), is(VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test
    public void testValidSinglePipelineEmptySourceSettings() {
        final PipelineParser pipelineParser = new PipelineParser(VALID_SINGLE_PIPELINE_EMPTY_SOURCE_PLUGIN_FILE);
        final Map<String, Pipeline> actualPipelineMap = pipelineParser.parseConfiguration();
        assertThat(actualPipelineMap.keySet().size(), is(1));
    }

    @Test //not preferring expected to validate exception message
    public void testCyclesInMultiplePipelines() {
        final PipelineParser pipelineParser = new PipelineParser(CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try {
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Provided configuration results in a loop, check pipeline: test-pipeline-1"));
        }
    }

    @Test //not preferring expected to validate exception message
    public void testInCorrectSourceMappingInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try {
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, expected source test-pipeline-1 for pipeline test-pipeline-2 is missing"));
        }
    }

    @Test
    public void testMissingNameInPipelineTypeInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE);
        try {
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("name is a required attribute for sink pipeline plugin, " +
                    "check pipeline: test-pipeline-1"));
        }
    }

    @Test
    public void testMissingPipelineTypeInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try {
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, no pipeline is defined with name test-pipeline-4"));
        }
    }

    @Test
    public void testMultipleSinks() {
        final PipelineParser pipelineParser = new PipelineParser(VALID_MULTIPLE_SINKS_CONFIG_FILE);
        pipelineParser.parseConfiguration();
    }

    @Test
    public void testMultipleProcessors() {
        final PipelineParser pipelineParser = new PipelineParser(VALID_MULTIPLE_PROCESSORS_CONFIG_FILE);
        pipelineParser.parseConfiguration();
    }

    @Test
    public void testIncorrectConfigurationFilePath() {
        try {
            final PipelineParser pipelineParser = new PipelineParser("file_does_no_exist.yml");
            pipelineParser.parseConfiguration();
        } catch (ParseException ex) {
            assertThat(ex.getMessage(), is("Failed to parse the configuration file file_does_no_exist.yml"));
        }
    }

    @Test
    public void testParseExceptionCreation() {
        try {
            throw new ParseException("TEST MESSAGE");
        } catch (ParseException ex) {
            assertThat(ex.getMessage(), is("TEST MESSAGE"));
        }

        final Throwable throwable = new Throwable();
        try {
            throw new ParseException(throwable);
        } catch (ParseException ex) {
            assertThat(ex.getCause(), notNullValue());
            assertThat(ex.getCause(), is(equalTo(throwable)));
        }
    }
}
