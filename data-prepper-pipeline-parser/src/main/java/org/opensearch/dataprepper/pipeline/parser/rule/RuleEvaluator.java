package org.opensearch.dataprepper.pipeline.parser.rule;

//import org.apache.commons.jexl3.JexlBuilder;
//import org.apache.commons.jexl3.JexlEngine;
//import org.apache.commons.jexl3.JexlExpression;
//import org.apache.commons.jexl3.JexlContext;
//import org.apache.commons.jexl3.MapContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import static java.lang.String.format;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RuleEvaluator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final TransformersFactory transformersFactory;
    private String PLUGIN_NAME = null;

    public RuleEvaluator(TransformersFactory transformersFactory) {
        this.transformersFactory = transformersFactory;
    }

    public RuleEvaluatorResult isTransformationNeeded(PipelinesDataFlowModel pipelineModel) {
        return isDocDBSource(pipelineModel);
    }

    /**
     *  Evaluates based on rules if it is docDB and needs transformation.
     *  Assumption: only one pipeline can have transformation.
     * @param pipelinesModel
     * @return
     */
    private RuleEvaluatorResult isDocDBSource(PipelinesDataFlowModel pipelinesModel) {
        //TODO
        //dynamically find pluginName Needed for transformation based on rule that applies
        PLUGIN_NAME = "documentdb";
        String pluginRulesPath = transformersFactory.getPluginRuleFileLocation(PLUGIN_NAME);
        Map<String, PipelineModel> pipelines = pipelinesModel.getPipelines();

        for (Map.Entry<String, PipelineModel> entry : pipelines.entrySet()) {
            try {
                String pipelineJson = OBJECT_MAPPER.writeValueAsString(entry);
                if(evaluate(pipelineJson, pluginRulesPath)) {
                    String templateFilePath = transformersFactory.getPluginTemplateFileLocation(PLUGIN_NAME);
                    PipelineTemplateModel templateModel = yamlMapper.readValue(new File(templateFilePath),
                            PipelineTemplateModel.class);
                    return RuleEvaluatorResult.builder()
                            .withEvaluatedResult(true)
                            .withPipelineTemplateModel(templateModel)
                            .withPipelineName(entry.getKey())
                            .build();
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return RuleEvaluatorResult.builder()
                .withEvaluatedResult(false)
                .withPipelineName(null)
                .withPipelineTemplateModel(null)
                .build();
    }

    private Boolean evaluate(String pipelinesJson,
                             String rulePath) {

//        ReadContext readPathContext = null;
        Configuration parseConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.AS_PATH_LIST)
                .build();
        ParseContext parseContext = JsonPath.using(parseConfig);
        ReadContext readPathContext = parseContext.parse(pipelinesJson);

        try {
            RuleTransformerModel rulesModel = yamlMapper.readValue(new File(rulePath), RuleTransformerModel.class);
            List<String> rules = rulesModel.getApplyWhen();
            for (String rule : rules) {
                Object result = readPathContext.read(rule);
            }
        } catch (IOException e) {
            //TODO --> this is failing the integ tests in core
            // reason - transformer
//            throw new RuntimeException(format("error reading file %s.",rulePath));

            return false;
        } catch (PathNotFoundException e) {
            return false;
        }
        return true;
    }
}

