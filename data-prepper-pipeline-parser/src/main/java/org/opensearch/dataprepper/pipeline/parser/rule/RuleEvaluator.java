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
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import static java.lang.String.format;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleTransformerModel;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RuleEvaluator {
//    private final JexlEngine jexlEngine;
    private final String templateFileNamePattern = "-template.yaml";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final PipelinesDataFlowModel pipelineModel;
    ParseContext parseContext = JsonPath.using(Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.ALWAYS_RETURN_LIST)
            .build());


    public RuleEvaluator(PipelinesDataFlowModel pipelineModel){
        this.pipelineModel = pipelineModel;
    }
    public boolean isTransformationNeeded() {
        return isDocDBSource();
    }

    private boolean isDocDBSource() {
        try {
            String pipelinesJson = OBJECT_MAPPER.writeValueAsString(this.pipelineModel);

            //Read from rule file
            List<String> rules = List.of(
                    "$..source[?(@.documentdb != null)]",
                    "$..source.documentdb.collections[0][?(!@.s3_bucket != null)]"
            );

            return evaluate(pipelinesJson,rules);

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private  boolean evaluate(String pipelinesJson,List<String> rules){
        ParseContext parseContext = JsonPath.using(Configuration.builder()
                .jsonProvider(new JacksonJsonProvider())
                .mappingProvider(new JacksonMappingProvider())
                .options(Option.ALWAYS_RETURN_LIST)
                .build());
        ReadContext readPathContext = parseContext.parse(pipelinesJson);
        Boolean evaluatedResult = true;

        for(String rule:rules){
            try {
                List<?> result = readPathContext.read(rule);
                if(result.isEmpty() || result ==null){
                    evaluatedResult = false;
                }
            }catch(Exception e){
                //TODO
//                LOG.error(format("Unable to find rule %s in the pipeline model json",rule));
                return false;
            }
        }
        return evaluatedResult;
    }

    public String getTemplateJsonString(){
        String filePath = "src/resources/templates/documentdb-template.yml";
        String templateJsonString = null;

        //read file as json string based on a mapper file


        return templateJsonString;
    }




//
//    public String getTemplateFileLocationForTransformation(RuleConfig rule){
//
//        String expressionStr = rule.getApplyWhen();
//        final String pluginName = getPluginNameThatNeedsTransformation(expressionStr);
//        final String templateFileName = pluginName+templateFileNamePattern;
//
//        //TODO - scan for templateFileName in class path
//        String templateFilePath = "src/resources/" + pluginName + "/templates/" + templateFileName;
//        return templateFilePath;
//    }
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

