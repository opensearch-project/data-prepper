package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluatorResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicConfigTransformer implements PipelineConfigurationTransformer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RuleEvaluator ruleEvaluator;
    private final PipelinesDataflowModelParser pipelinesDataflowModelParser;
    private final PipelinesDataFlowModel preTransformedPipelinesDataFlowModel;
    Pattern placeholderPattern = Pattern.compile("\\{\\{\\s*(.+?)\\s*}}");

    // Configuration necessary for JsonPath to work with Jackson
    Configuration parseConfig = Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .build();

    Configuration parseConfigWithJsonNode = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS)
            .build();

    ParseContext mainParseContext = JsonPath.using(parseConfig);


    public DynamicConfigTransformer(PipelinesDataflowModelParser pipelinesDataflowModelParser,
                                    RuleEvaluator ruleEvaluator) {
        this.ruleEvaluator = ruleEvaluator;
        this.pipelinesDataflowModelParser = pipelinesDataflowModelParser;
        this.preTransformedPipelinesDataFlowModel = pipelinesDataflowModelParser.parseConfiguration();
    }

    //TODO
    // address pipeline_configurations and version
    // address sub-piplines
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelineTemplateModel templateModel) {
        RuleEvaluatorResult ruleEvaluatorResult =  ruleEvaluator.isTransformationNeeded(preTransformedPipelinesDataFlowModel);

        if (ruleEvaluatorResult.isEvaluatedResult() == false) {
            return preTransformedPipelinesDataFlowModel;
        }

        String pipelineNameThatNeedsTransformation = ruleEvaluatorResult.getPipelineName();
        try {
//            String pipelineNameThatNeedsTransformation; //TODO

            String templateJsonString = objectMapper.writeValueAsString(templateModel);
            Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
            pipelines.get(pipelineNameThatNeedsTransformation);
            String pipelinesDataFlowModelJsonString = objectMapper.writeValueAsString(preTransformedPipelinesDataFlowModel);
            ReadContext pipelineReadContext = mainParseContext.parse(pipelinesDataFlowModelJsonString);

            //find all placeholderPattern in template json string
            // K:placeholder , V:jsonPath
            Map<String, String> placeholdersMap = findPlaceholdersWithPaths(templateJsonString);

            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);

            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder, templateJsonPath) -> {
                String pipelineJsonPath = getValueFromPlaceHolder(placeholder);
//                JsonNode pipelineNode1 = pipelineReadContext.read(pipelineJsonPath, JsonNode.class);

                JsonNode pipelineNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelinesDataFlowModelJsonString).read(pipelineJsonPath);

//                //TODO --> had changed these 3 lines so that i dont get a ArrayNode on query every time; but this seems to lead to some RuntimeException.
//                String parentPath = templateJsonPath.substring(0, templateJsonPath.lastIndexOf('.'));
//                String fieldName = templateJsonPath.substring(templateJsonPath.lastIndexOf('.') + 1);
//                JsonNode pipelineNode3 = JsonPath.using(parseConfigWithJsonNode).parse(templateRootNode).read(parentPath);

                //replace pipelineNode in the template
                replaceNode(templateRootNode, templateJsonPath, pipelineNode);
            });

            //update template json
            String transformedJson = objectMapper.writeValueAsString(templateRootNode);

            //transform transformedJson to pipelinesModels
            PipelinesDataFlowModel transformedPipelinesDataFlowModel = objectMapper.readValue(transformedJson, PipelinesDataFlowModel.class);

            return transformedPipelinesDataFlowModel;

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> findPlaceholdersWithPaths(String json) throws IOException {

        JsonNode rootNode = objectMapper.readTree(json);
        Map<String, String> placeholdersWithPaths = new HashMap<>();
        walkJson(rootNode, "", placeholdersWithPaths);
        return placeholdersWithPaths;
    }

    private void walkJson(JsonNode currentNode, String currentPath, Map<String, String> placeholdersWithPaths) {
        if (currentNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = currentNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String path = currentPath.isEmpty() ? entry.getKey() : currentPath + "." + entry.getKey();
                walkJson(entry.getValue(), path, placeholdersWithPaths);
            }
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                String path = currentPath + "[" + i + "]";
                walkJson(currentNode.get(i), path, placeholdersWithPaths);
            }
        } else if (currentNode.isValueNode()) {
            String placeHolderValue = currentNode.asText();
            Matcher matcher = placeholderPattern.matcher(placeHolderValue);
            if (matcher.find()) {
                placeholdersWithPaths.put(placeHolderValue, currentPath);
            }
        }
    }

    private String getValueFromPlaceHolder(String placeholder) {
        if (placeholder.length() < 4) {
            //TODO
            //invalid placeholder
            throw new RuntimeException();
        }

        //remove the first 2 and last 2 characters.
        return placeholder.substring(2, placeholder.length() - 2);
    }

    public void replaceNode(JsonNode root, String jsonPath, JsonNode newNode) {
        try {
            // Read the parent path of the target node
            //TODO
            String parentPath = jsonPath.substring(0, jsonPath.lastIndexOf('.'));
            String fieldName = jsonPath.substring(jsonPath.lastIndexOf('.') + 1);

            // Find the parent node
            JsonNode parentNode = JsonPath.using(parseConfigWithJsonNode).parse(root).read(parentPath);

            // Replace the target field in the parent node
            if (parentNode != null && parentNode instanceof ObjectNode) {
                ((ObjectNode) parentNode).replace(fieldName, newNode);
            } else {
                throw new IllegalArgumentException("Path does not point to an object node");
            }
        } catch (PathNotFoundException e) {
            throw new PathNotFoundException(e);
        }
    }
}
