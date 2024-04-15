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
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
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

    private RuleEvaluatorResult isDocDBSource(PipelinesDataFlowModel pipelinesModel) {
        //TODO
        //dynamically find pluginName Needed for transformation based on rule that applies
        PLUGIN_NAME = "documentdb";
        RuleEvaluatorResult result = new RuleEvaluatorResult();
//            String pipelinesJson = OBJECT_MAPPER.writeValueAsString(pipelineModel);
        String pluginRulesPath = transformersFactory.getPluginRuleFileLocation(PLUGIN_NAME);
        Map<String, PipelineModel> pipelines = pipelinesModel.getPipelines();

        for (Map.Entry<String, PipelineModel> entry : pipelines.entrySet()) {
            try {
                String pipelineJson = OBJECT_MAPPER.writeValueAsString(entry);
                return RuleEvaluatorResult.builder()
                        .withEvaluatedResult(evaluate(pipelineJson, pluginRulesPath))
                        .withPipelineName(entry.getKey())
                        .build();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return RuleEvaluatorResult.builder()
                .withEvaluatedResult(false)
                .withPipelineName(null)
                .build();
    }

    public String getTransformationTemplate() {
        return transformersFactory.getPluginTemplateFileLocation(PLUGIN_NAME);
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
            throw new RuntimeException();
        } catch (PathNotFoundException e) {
            return false;
        }
        return true;
    }

//    private String formValidTemplefilePathFromValidRule(String validRuleFilename) {
//        String[] splitRuleString = validRuleFilename.split("-");
//        String pluginName = splitRuleString[0];
//        return pluginName + TEMPLATE_FILE_PATTERN;
//    }


//
//
//    /**
//     * Assumption: The rule is always of this format: "(pipeline.<source/processor>.PluginName)"
//     *
//     */
//    private String getPluginNameThatNeedsTransformation(String expressionStr) {
//
//        //checking for rule pattern
//        Pattern pattern = Pattern.compile("pipeline\\.(.*?)\\.(.*)\\)");
//        Matcher matcher = pattern.matcher(expressionStr);
//
//        if (matcher.find()) {
//            // Extract the 'PluginName' part
//            String pluginName = matcher.group(2);
//
//            return pluginName;
//        } else {
//            throw new RuntimeException("Invalid rule expression format: " + expressionStr);
//        }
//
//    }

}

