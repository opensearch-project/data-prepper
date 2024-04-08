package org.opensearch.dataprepper.pipeline.parser.rule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PipelineExtensions;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;

class RuleEvaluatorTest {
    private RuleEvaluator ruleEvaluator;

    @BeforeEach
    void setUp() {
        ruleEvaluator = new RuleEvaluator();
    }

    @Test
    void testIsRuleValidTrue() {

        // Set up
        RuleConfig ruleConfig = new RuleConfig();
        ruleConfig.setApplyWhen("exists(pipeline.source.documentdb)");

        String pipelineName = "test-pipeline";
        final PluginModel source = new PluginModel("documentdb", (Map<String, Object>) null);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null, Collections.emptyList(), Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(
                (PipelineExtensions) null, Collections.singletonMap(pipelineName, pipelineModel));


        // Execute
        boolean isValid = ruleEvaluator.isRuleValid(ruleConfig, pipelinesDataFlowModel);

        // Assert
        assertTrue(isValid);
    }

    @Test
    void testIsRuleValidFalse() {
        // Similar setup to testIsRuleValidTrue but with conditions that result in false
    }

    @Test
    void testIsRuleValidThrowsExceptionForInvalidResultType() {
        // Setup a rule that results in a non-boolean value and assert it throws IllegalArgumentException
    }

    @Test
    void testGetPluginNameThatNeedsTransformation() {
        String expression = "pipeline.source.PluginName == 'documentdb'";
        RuleConfig ruleConfig = new RuleConfig();
        ruleConfig.setApplyWhen(expression);

//        String pluginName = ruleEvaluator.getPluginNameThatNeedsTransformation(ruleConfig.getApplyWhen());

//        assertEquals("PluginName", pluginName);
    }

    @Test
    void testGetPluginNameThatNeedsTransformationInvalidFormat() {
        // Test with an invalid expression format to ensure it throws a RuntimeException
    }

    // You can add more tests to cover edge cases and exceptional scenarios.
}
