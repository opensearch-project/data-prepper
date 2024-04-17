package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PipelineTemplateModelTest {

    private PipelineTemplateModel pipelineTemplateModel;
    private Map<String, Object> pipelines;

    @BeforeEach
    public void setup() {
        pipelines = new HashMap<>();
        pipelines.put("examplePipeline", new Object());
        pipelineTemplateModel = new PipelineTemplateModel(pipelines);
    }

    @Test
    public void testConstructor_initializesMap() {
        assertNotNull(pipelineTemplateModel.getTemplatePipelines());
        assertEquals(1, pipelineTemplateModel.getTemplatePipelines().size());
        assertTrue(pipelineTemplateModel.getTemplatePipelines().containsKey("examplePipeline"));
    }

    @Test
    public void testGetTemplatePipelines_returnsCorrectMap() {
        Map<String, Object> retrievedMap = pipelineTemplateModel.getTemplatePipelines();
        assertEquals(pipelines, retrievedMap);
    }

    @Test
    public void testJsonAnySetter_addsDynamicProperties() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = "{\"extraProperty\":\"value\", \"anotherProperty\":123}";

        PipelineTemplateModel modelFromJson = objectMapper.readValue(json, PipelineTemplateModel.class);
        assertNotNull(modelFromJson.getTemplatePipelines());
        assertEquals("value", modelFromJson.getTemplatePipelines().get("extraProperty"));
        assertEquals(123, modelFromJson.getTemplatePipelines().get("anotherProperty"));
    }
}
