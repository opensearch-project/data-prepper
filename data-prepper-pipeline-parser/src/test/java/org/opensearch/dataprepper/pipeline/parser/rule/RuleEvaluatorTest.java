package org.opensearch.dataprepper.pipeline.parser.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;

class RuleEvaluatorTest {
    private RuleEvaluator ruleEvaluator;

    @Test
    void test_isTransformationNeeded_ForDocDBSource_ShouldReturn_False() {

        // Set up
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String,Object>();
        Map s3_bucket = new HashMap<>();
        s3_bucket.put("s3_bucket", "bucket-name");
        List collections = new ArrayList();
        collections.add(s3_bucket);
        sourceOptions.put("collections", collections);
        final PluginModel source = new PluginModel("documentdb", sourceOptions );
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        ruleEvaluator = new RuleEvaluator(pipelinesDataFlowModel);
        // Execute
        boolean isValid = ruleEvaluator.isTransformationNeeded();

        // Assert
        assertTrue(isValid);
    }

    @Test
    void test_isTransformationNeeded_ForOtherSource_ShouldReturn_False() {
        // Similar setup to testIsRuleValidTrue but with conditions that result in false

        // Set up
        String pipelineName = "test-pipeline";
        Map sourceOptions = new HashMap<String,Object>();
        sourceOptions.put("option1", "1");
        final PluginModel source = new PluginModel("http", sourceOptions );
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));

        ruleEvaluator = new RuleEvaluator(pipelinesDataFlowModel);
        // Execute
        boolean isValid = ruleEvaluator.isTransformationNeeded();

        // Assert
        assertEquals(isValid, false);
    }

    @Test
    void testIsRuleValidThrowsExceptionForInvalidResultType() {
        // Setup a rule that results in a non-boolean value and assert it throws IllegalArgumentException
    }

    @Test
    void testGetPluginNameThatNeedsTransformation() {

//        String pluginName = ruleEvaluator.getPluginNameThatNeedsTransformation(ruleConfig.getApplyWhen());

//        assertEquals("PluginName", pluginName);
    }

    @Test
    void testGetPluginNameThatNeedsTransformationInvalidFormat() {
        // Test with an invalid expression format to ensure it throws a RuntimeException
    }

    // You can add more tests to cover edge cases and exceptional scenarios.
}
