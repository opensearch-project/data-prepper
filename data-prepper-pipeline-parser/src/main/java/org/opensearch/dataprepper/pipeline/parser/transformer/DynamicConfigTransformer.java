package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
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

//    Pattern pipelineNamePlaceholderPattern = Pattern.compile("\\{\\{\\s*(.+?)\\s*}}");
    String pipelineNamePlaceholderRegex = "\\{\\{\\s*" + Pattern.quote("pipeline-name") + "\\s*\\}\\}";
    String templatePipelineRootString = "templatePipelines";

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

    /**
     *
     * @param templateModel
     * @return
     */
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelineTemplateModel templateModel) {
        RuleEvaluatorResult ruleEvaluatorResult = ruleEvaluator.isTransformationNeeded(preTransformedPipelinesDataFlowModel);

        if (ruleEvaluatorResult.isEvaluatedResult() == false ||
        ruleEvaluatorResult.getPipelineName() == null) {
            return preTransformedPipelinesDataFlowModel;
        }

        //To differentiate between sub-pipelines that dont need transformation.
        String pipelineNameThatNeedsTransformation = ruleEvaluatorResult.getPipelineName();
        try {


            Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
            Map<String, PipelineModel> pipelineMap = new HashMap<>();
            pipelineMap.put(pipelineNameThatNeedsTransformation,
                    pipelines.get(pipelineNameThatNeedsTransformation));
            String pipelineJson = objectMapper.writeValueAsString(pipelineMap);


            String templateJsonStringWithPipelinePlaceholder = objectMapper.writeValueAsString(templateModel);
            String templateJsonString = replaceTemplatePipelineName(templateJsonStringWithPipelinePlaceholder,
                    pipelineNameThatNeedsTransformation);
            //find all placeholderPattern in template json string
            // K:placeholder , V:jsonPath in templateJson
            Map<String, String> placeholdersMap = findPlaceholdersWithPaths(templateJsonString);
            validateAllPlaceHoldersFound(placeholdersMap, templateJsonString);
            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);


            // get exact path in pipelineJson - this is to avoid
            // getting array values(even though it might not be an array) given
            // a recursive expression like "$..<>"
            // K:jsonPath, V:exactPath
            Map<String, String> pipelineExactPathMap = findExactPath(placeholdersMap, pipelineNameThatNeedsTransformation);


            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder, templateJsonPath) -> {
                String pipelineExactJsonPath = pipelineExactPathMap.get(placeholder);
                JsonNode pipelineNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelineJson).read(pipelineExactJsonPath);
                replaceNode(templateRootNode, templateJsonPath, pipelineNode);
            });

            //update template json
            String transformedJson = objectMapper.writeValueAsString(templateRootNode);

            //convert TransformedJson to PipelineModel with the data from preTransformedDataFlowModel.
            //transform transformedJson to Map
            Map<String,Object> transformedConfigMap = objectMapper.readValue(transformedJson, Map.class);


            // get the root of the Transformed Pipeline Model, to get the actual pipelines.
            // direct conversion to PipelineDataModel throws exception.
            Map<String, PipelineModel> transformedPipelines = (Map<String, PipelineModel>) transformedConfigMap.get(templatePipelineRootString);
            pipelines.forEach((pipelineName, pipeline)->{
                if(!pipelineName.equals(pipelineNameThatNeedsTransformation)){
                    transformedPipelines.put(pipelineName,pipeline);
                }
            });
            PipelinesDataFlowModel transformedPipelinesDataFlowModel = new PipelinesDataFlowModel(
                    preTransformedPipelinesDataFlowModel.getDataPrepperVersion(),
                    preTransformedPipelinesDataFlowModel.getPipelineExtensions(),
                    transformedPipelines
            );
            return transformedPipelinesDataFlowModel;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String replaceTemplatePipelineName(String templateJsonStringWithPipelinePlaceholder, String pipelineName) {
        return templateJsonStringWithPipelinePlaceholder.replaceAll(pipelineNamePlaceholderRegex, pipelineName);
    }

    private void validateAllPlaceHoldersFound(Map<String, String> placeholdersMap, String templateJson) {
        assert (placeholdersMap.size() == countMatches(templateJson));
    }
//
//    private String getExactPath(String pipelineJson, String pipelineGenericJsonPath) {
//        try {
//            JsonNode pipelineRootNode = objectMapper.readTree(pipelineJson);
//            List<JsonNode> initialNodes = JsonPath.read(pipelineRootNode.toString(), pipelineGenericJsonPath);
//
//            Map<String, List<String>> pathsMap = new HashMap<>();
//            initialNodes.forEach(node -> findPaths(pipelineRootNode, node.asText(), "", pathsMap));
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//
//
//        return null;
//    }

    private Map<String, String> findPlaceholdersWithPaths(String json) throws IOException {

        JsonNode rootNode = objectMapper.readTree(json);
        Map<String, String> placeholdersWithPaths = new HashMap<>();
        populateMapWithPlaceholderPaths(rootNode, "", placeholdersWithPaths);
        return placeholdersWithPaths;
    }

    private void populateMapWithPlaceholderPaths(JsonNode currentNode, String currentPath, Map<String, String> placeholdersWithPaths) {
        if (currentNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = currentNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String path = currentPath.isEmpty() ? entry.getKey() : currentPath + "." + entry.getKey();
                populateMapWithPlaceholderPaths(entry.getValue(), path, placeholdersWithPaths);
            }
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                String path = currentPath + "[" + i + "]";
                populateMapWithPlaceholderPaths(currentNode.get(i), path, placeholdersWithPaths);
            }
        } else if (currentNode.isValueNode()) {
            String placeHolderValue = currentNode.asText();
            Matcher matcher = placeholderPattern.matcher(placeHolderValue);
            if (matcher.find()) {
                placeholdersWithPaths.put(placeHolderValue, currentPath);
            }
        }
    }

    /**
     *
     * @param placeholdersMap
     * @param pipelineName
     * @return
     * @throws IOException
     */
    private Map<String, String> findExactPath(Map<String, String> placeholdersMap, String pipelineName) throws IOException {
        Map<String, String> mapWithPaths = new HashMap<>();
        for (String genericPathPlaceholder: placeholdersMap.keySet()){
            String genericPath = getValueFromPlaceHolder(genericPathPlaceholder);
            if(genericPath.contains("$.*.")){
                String exactPath = genericPath.replace("$.*.","$."+pipelineName+".");
                mapWithPaths.put(genericPathPlaceholder,exactPath);
            }
        }
        return mapWithPaths;
    }
//
//    private void populateMapWithExactPath(JsonNode currentNode, String currentPath, Map<String, String> mapWithPaths) {
//        if (currentNode.isObject()) {
//            //Iterate all nodes in array
//            Iterator<Map.Entry<String, JsonNode>> fields = currentNode.fields();
//            while (fields.hasNext()) {
//                Map.Entry<String, JsonNode> entry = fields.next();
//                String path = currentPath.isEmpty() ? entry.getKey() : currentPath + "." + entry.getKey();
//                populateMapWithPlaceholderPaths(entry.getValue(), path, mapWithPaths);
//            }
//        } else if (currentNode.isArray()) {
//            //Iterate all nodes in array
//            for (int i = 0; i < currentNode.size(); i++) {
//                String path = currentPath + "[" + i + "]";
//                populateMapWithPlaceholderPaths(currentNode.get(i), path, mapWithPaths);
//            }
//        } else if (currentNode.isValueNode()) {
//            //Dont do anything if it is a Value Node.
//            String placeHolderValue = currentNode.asText();
//            Matcher matcher = placeholderPattern.matcher(placeHolderValue);
//            if (matcher.find()) {
//                mapWithPaths.put(placeHolderValue, currentPath);
//            }
//        }
//    }

    private String getValueFromPlaceHolder(String placeholder) {
        if (placeholder.length() < 4) {
            throw new RuntimeException("Invalid placeholder value");
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

    private int countMatches(String json) {
        Matcher matcher = placeholderPattern.matcher(json);

        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
