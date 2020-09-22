package com.amazon.situp.parser;

import com.amazon.situp.pipeline.Pipeline;
import org.junit.Test;

import java.util.Map;

import static com.amazon.situp.TestDataProvider.CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.VALID_MULTIPLE_PIPELINE_CONFIG_FILE;
import static com.amazon.situp.TestDataProvider.VALID_MULTIPLE_PIPELINE_NAMES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PipelineParserTests {

    @Test
    public void testValidMultiplePipelines() {
        final PipelineParser pipelineParser = new PipelineParser(VALID_MULTIPLE_PIPELINE_CONFIG_FILE);
        final Map<String, Pipeline> actualPipelineMap = pipelineParser.parseConfiguration();
        assertThat(actualPipelineMap.keySet(), is(VALID_MULTIPLE_PIPELINE_NAMES));
    }

    @Test //not preferring expected to validate exception message
    public void testCyclesInMultiplePipelines() {
        final PipelineParser pipelineParser = new PipelineParser(CYCLE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try{
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Provided configuration results in a loop, check pipeline: test-pipeline-1"));
        }
    }

    @Test //not preferring expected to validate exception message
    public void testInCorrectSourceMappingInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(INCORRECT_SOURCE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try{
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, expected source test-pipeline-1 for pipeline test-pipeline-2 is missing"));
        }
    }

    @Test
    public void testMissingNameInPipelineTypeInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(MISSING_NAME_MULTIPLE_PIPELINE_CONFIG_FILE);
        try{
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("name is a required attribute for sink pipeline plugin, " +
                    "check pipeline: test-pipeline-1"));
        }
    }

    @Test
    public void testMissingPipelineTypeInMultiPipelines() {
        final PipelineParser pipelineParser = new PipelineParser(MISSING_PIPELINE_MULTIPLE_PIPELINE_CONFIG_FILE);
        try{
            pipelineParser.parseConfiguration();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Invalid configuration, no pipeline is defined with name test-pipeline-4"));
        }
    }
}
