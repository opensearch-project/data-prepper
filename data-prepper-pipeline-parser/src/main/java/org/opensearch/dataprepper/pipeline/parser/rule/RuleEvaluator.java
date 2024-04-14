package org.opensearch.dataprepper.pipeline.parser.rule;

//import org.apache.commons.jexl3.JexlBuilder;
//import org.apache.commons.jexl3.JexlEngine;
//import org.apache.commons.jexl3.JexlExpression;
//import org.apache.commons.jexl3.JexlContext;
//import org.apache.commons.jexl3.MapContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class RuleEvaluator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final TransformersFactory transformersFactory;
    private String PLUGIN_NAME = null;

    public RuleEvaluator(TransformersFactory transformersFactory) {
        this.transformersFactory = transformersFactory;
    }

    public boolean isTransformationNeeded(PipelinesDataFlowModel pipelineModel) {
        return isDocDBSource(pipelineModel);
    }

    private boolean isDocDBSource(PipelinesDataFlowModel pipelineModel) {
        //TODO
        //dynamically find pluginName Needed for transformation based on rule that applies
        PLUGIN_NAME = "documentdb";
        try {
            String pipelinesJson = OBJECT_MAPPER.writeValueAsString(pipelineModel);
            String pluginRulesPath = transformersFactory.getPluginRuleFileLocation(PLUGIN_NAME);

            return evaluate(pipelinesJson, pluginRulesPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getTransformationTemplate(){
        return transformersFactory.getPluginTemplateFileLocation(PLUGIN_NAME);
    }

    private boolean evaluate(String pipelinesJson, String rulePath) {

//        ReadContext readPathContext = null;
        Configuration parseConfig = Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.AS_PATH_LIST)
                .build();
        ParseContext parseContext = JsonPath.using(parseConfig);
        ReadContext readPathContext = parseContext.parse(pipelinesJson);

        // for every file, apply all rules in file. if all rules in a file are true,
        // then pick the filename and get the pluginName
        // get template from the pluginName.

        try {
            RuleTransformerModel rulesModel = yamlMapper.readValue(new File(rulePath), RuleTransformerModel.class);
            List<String> rules = rulesModel.getApplyWhen();
            for (String rule : rules) {
                Object result = readPathContext.read(rule);
            }
        } catch (IOException e) {
            throw new RuntimeException();
        } catch (PathNotFoundException e){
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

