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
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;
import org.opensearch.dataprepper.pipeline.parser.transformer.TransformersFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class RuleEvaluator {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEMPLATE_FILE_PATTERN = "-template.yaml";
    private final TransformersFactory transformersFactory;
    private final String PLUGIN_NAME = null;
    private String validTemplateFilePath = null;
    private String validRuleFilePath = null;

    public RuleEvaluator(TransformersFactory transformersFactory) {
        this.transformersFactory = transformersFactory;
    }

    public boolean isTransformationNeeded(PipelinesDataFlowModel pipelineModel) {
        return isDocDBSource(pipelineModel);
    }

    private boolean isDocDBSource(PipelinesDataFlowModel pipelineModel) {
        //TODO
        //dynamically find pluginName Needed for transformation based on rule that applies
        String PLUGIN_NAME = "documentdb";
        try {
            String pipelinesJson = OBJECT_MAPPER.writeValueAsString(pipelineModel);
            String pluginRulesPath = transformersFactory.getPluginRuleFileLocation(PLUGIN_NAME);

            //Read from rule file
//            List<String> rulesFiles = Files.readAllLines(Paths.get(pluginRulesPath));
            final File directory = new File(pluginRulesPath);
            final File[] rulesDefinitionFiles = directory.listFiles();

//            List<String> rules = List.of(
//                    "$..source[?(@.documentdb != null)]",
//                    "$..source.documentdb.collections[0][?(!@.s3_bucket != null)]"
//            );

            return evaluate(pipelinesJson, rulesDefinitionFiles);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean evaluate(String pipelinesJson, List<File> rulesFiles) {

        String validRuleFile = null;
        ReadContext readPathContext = null;
        ParseContext parseContext = JsonPath.using(Configuration.builder().jsonProvider(new JacksonJsonProvider()).mappingProvider(new JacksonMappingProvider())
//                .options(Option.ALWAYS_RETURN_LIST) //TODO might not be needed ; removing might simplify the rule evaluation
                .build());
        try {
            readPathContext = parseContext.parse(pipelinesJson);
        } catch (Exception e) {
            return false;
        }

        // for every file, apply all rules in file. if all rules in a file are true,
        // then pick the filename and get the pluginName
        // get template from the pluginName.

        try {
            for (File ruleFile : rulesFiles) {
                List<String> rules = Files.readAllLines(ruleFile.toPath());
                for (String rule : rules) {

                    Object result = readPathContext.read(rule);
                    if (result != null) {
                        setValidRuleFilePath(ruleFile.getName());
                        String validTemplateFilePath = formValidTemplefilePathFromValidRule(ruleFile.getName());
                        setValidTemplateFilePath(validTemplateFilePath);
                        return true;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException();
        }
        return false;
    }

    private String formValidTemplefilePathFromValidRule(String validRuleFilename){
        String[] splitRuleString = validRuleFilename.split("-");
        String pluginName = splitRuleString[0];
        setPluginName(pluginName);
        return pluginName + TEMPLATE_FILE_PATTERN;
    }

    private void setPluginName(String pluginName) {
        this.PLUGIN_NAME = pluginName;
    }

    private void setValidRuleFilePath(String ruleFilePath) {
        this.validRuleFilePath = ruleFilePath;
    }

    private String getValidTemplateFilePath() {
        return validTemplateFilePath;
    }

    private String getValidRuleFilePath() {
        return validRuleFilePath;
    }

    private void setValidTemplateFilePath(String templateFilePath) {
        this.validTemplateFilePath = templateFilePath;
    }

    //TODO OOOO
    public PipelineTemplateModel getTemplateModel(String pluginName) {
        String templatePath = transformersFactory.getTransformationTemplateFileLocation();

        //read file as json string based on a mapper file
        try {
            PipelineTemplateModel pipelineTemplateModel = YAML_MAPPER.readValue(new File(templatePath), PipelineTemplateModel.class);
            return pipelineTemplateModel;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


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

