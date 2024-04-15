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
import java.util.List;
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

    /**
     *
     * @param templateModel
     * @return
     */
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelineTemplateModel templateModel) {
        RuleEvaluatorResult ruleEvaluatorResult = ruleEvaluator.isTransformationNeeded(preTransformedPipelinesDataFlowModel);

        if (ruleEvaluatorResult.isEvaluatedResult() == false) {
            return preTransformedPipelinesDataFlowModel;
        }

        //To differentiate between sub-pipelines.
        String pipelineNameThatNeedsTransformation = ruleEvaluatorResult.getPipelineName();
        try {

            String templateJsonString = objectMapper.writeValueAsString(templateModel);
            Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
            Map<String, PipelineModel> pipelineMap = new HashMap<>();
            pipelineMap.put(pipelineNameThatNeedsTransformation,
                    pipelines.get(pipelineNameThatNeedsTransformation));
            String pipelineJson = objectMapper.writeValueAsString(pipelineMap);

            //find all placeholderPattern in template json string
            // K:placeholder , V:jsonPath in templateJson
            Map<String, String> placeholdersMap = findPlaceholdersWithPaths(templateJsonString);
            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);

            // get exact path in pipelineJson - this is to avoid
            // getting array values(even though it might not be an array) given
            // a recursive expression like "$..<>"
            // K:jsonPath expression V:exactPath(can be string or array)
            Map<String, Object> pipelineExactPathMap = findExactPath(placeholdersMap,pipelineJson);


            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder, templateJsonPath) -> {
                /**
                 * $..source for example always returns an array irrespective
                 * of the values source contains.
                 */
//                String pipelineGenericJsonPath = getValueFromPlaceHolder(placeholder);
//                JsonNode pipelineNode1 = pipelineReadContext.read(pipelineJsonPath, JsonNode.class);
//                String pipelineExactJsonPath = getExactPath(pipelineGenericJsonPath);

//                JsonNode pipelineNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelinesDataFlowModelJsonString).read(pipelineExactJsonPath);

//                //TODO --> had changed these 3 lines so that i dont get a ArrayNode on query every time; but this seems to lead to some RuntimeException.
//                String parentPath = templateJsonPath.substring(0, templateJsonPath.lastIndexOf('.'));
//                String fieldName = templateJsonPath.substring(templateJsonPath.lastIndexOf('.') + 1);
//                JsonNode pipelineNode3 = JsonPath.using(parseConfigWithJsonNode).parse(templateRootNode).read(parentPath);

                //replace pipelineNode in the template
//                replaceNode(templateRootNode, templateJsonPath, pipelineNode);
            });

            //update template json
            String transformedJson = objectMapper.writeValueAsString(templateRootNode);

            //transform transformedJson to pipelinesModels
            // PipelineModel transformedPipelineMode = objectMapper.readValue(transformedJson, PipelineModel.class);
            //


            // Map<String, PipelineModel> transformedPipelines = new HashMap<>;
            // put all except transformedPipelineName
            // Copy version and pipelineConfiguration as is.
            //
//            PipelinesDataFlowModel transformedPipelinesDataFlowModel = createTransformedPipelineDataFlowModel(preTransformedPipelinesDataFlowModel,
//                    transformedJson,pipelineNameThatNeedsTransformation);
//                    objectMapper.readValue(transformedJson, PipelinesDataFlowModel.class);

            //TODO
            return null;

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    private Map<String, String> findExactPath(String json) throws IOException {

        JsonNode rootNode = objectMapper.readTree(json);
        Map<String, String> mapWithPaths = new HashMap<>();
        populateMapWithPlaceholderPaths(rootNode, "", mapWithPaths);
        return mapWithPaths;
    }

    private void populateMapWithExactPath(JsonNode currentNode, String currentPath, Map<String, String> placeholdersWithPaths) {
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
