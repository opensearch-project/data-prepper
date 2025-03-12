/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import static java.lang.String.format;
import static org.opensearch.dataprepper.plugins.source.rds.RdsService.MAX_SOURCE_IDENTIFIER_LENGTH;

import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.model.configuration.SinkModel;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluatorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.opensearch.dataprepper.plugins.source.rds.utils.IdentifierShortener;
import software.amazon.awssdk.arns.Arn;

import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicConfigTransformer implements PipelineConfigurationTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicConfigTransformer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RuleEvaluator ruleEvaluator;

    /**
     * Placeholder will look like "<<placeholderValue>>"
     */
    private final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\<\\<\\s*(.+?)\\s*>>");
    /**
     * Placeholder used to find "<<pipeline-name>>"
     */
    private static final String PIPELINE_NAME_PLACEHOLDER_REGEX = "\\<\\<\\s*" + Pattern.quote("pipeline-name") + "\\s*\\>\\>";

    /**
     * This is the root node of the template json. This is got when converting template model to
     * corresponding json and will always be a constant
     */
    private static final String TEMPLATE_PIPELINE_ROOT_STRING = "templatePipelines";

    /**
     * placeholder for executing functions runtime based on template parameter
     * Example: <<FUNCTION_NAME:print, PARAMETER:helloWorld>>
     */
    private static final String FUNCTION_CALL_PLACEHOLDER_REGEX = "FUNCTION_NAME:(.*?),PARAMETER:(.*)";
    private final Pattern FUNCTION_CALL_PLACEHOLDER_PATTERN = Pattern.compile(FUNCTION_CALL_PLACEHOLDER_REGEX);

    /**
     * Json Path expression like "?(@.<node>)" seem to always return arrayNode even if it is an ObjectNode.
     * JSON_PATH_ARRAY_DISAMBIGUATOR_PATTERN is a way used to detect and disambiguate the path.
     */
    private static final String JSON_PATH_ARRAY_DISAMBIGUATOR_PATTERN = "[?(@.";
    private static final String RECURSIVE_JSON_PATH_PATH = "$..";
    private static final String JSON_PATH_IDENTIFIER = "$.";
    private static final String ARRAY_NODE_PATTERN = "([^\\[]+)\\[(\\d+)\\]$";
    private static final String SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE = "SOURCE_COORDINATION_PIPELINE_IDENTIFIER";
    private static final String SINK_SUBPIPELINE_PLUGIN_NAME = "pipeline";
    private static final String SUBPIPELINE_PATH = "$.source.pipeline";

    private static final String S3_BUFFER_PREFIX = "/buffer";

    Configuration parseConfigWithJsonNode = Configuration.builder()
            .jsonProvider(new JacksonJsonNodeJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.SUPPRESS_EXCEPTIONS)
            .build();

    public DynamicConfigTransformer(RuleEvaluator ruleEvaluator) {
        this.ruleEvaluator = ruleEvaluator;
    }

    /**
     * High Level Explanation:
     * Step1: Evaluate if transformation is needed
     * Step2: Create a Map(placeholdersMap) with key as placeholder and value as List of JsonPath
     * in templateJson. It is populated by recursively by tracking the placeholder and along the way,
     * store the paths.
     * Step3: Create a Map(pipelineExactPathMap) with exact path for the placeholder value in the
     * original pipelineJson.
     * Step4: For every placeholder, replace the template in the corresponding template json path with
     * node from the original pipelineJson.
     * Step5: Convert result to PipelinesDataFlowModel.
     *
     * @param preTransformedPipelinesDataFlowModel - represents the pre-transformed pipeline data flow model
     * @return PipelinesDataFlowModel - Transformed PipelinesDataFlowModel.
     */
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel preTransformedPipelinesDataFlowModel) {
        RuleEvaluatorResult ruleEvaluatorResult = ruleEvaluator.isTransformationNeeded(preTransformedPipelinesDataFlowModel);

        if (!ruleEvaluatorResult.isEvaluatedResult() ||
                ruleEvaluatorResult.getPipelineName() == null) {
            LOG.info("No transformation needed");
            return preTransformedPipelinesDataFlowModel;
        }

        //To differentiate between sub-pipelines that dont need transformation.
        String pipelineNameThatNeedsTransformation = ruleEvaluatorResult.getPipelineName();
        PipelineTemplateModel templateModel = ruleEvaluatorResult.getPipelineTemplateModel();
        LOG.info("Transforming pipeline config for pipeline {}",pipelineNameThatNeedsTransformation);
        try {

            Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
            List<String> subPipelineNames = new ArrayList<>();
            checkForSubPipelines(preTransformedPipelinesDataFlowModel, pipelineNameThatNeedsTransformation, subPipelineNames);
            Map<String, PipelineModel> pipelineMap = new HashMap<>();
            pipelineMap.put(pipelineNameThatNeedsTransformation,
                    pipelines.get(pipelineNameThatNeedsTransformation));
            String pipelineJson = objectMapper.writeValueAsString(pipelineMap);

            String templateJsonStringWithPipelinePlaceholder = objectMapper.writeValueAsString(templateModel);

            //Replace pipeline name placeholder with pipelineNameThatNeedsTransformation
            String templateJsonString = replaceTemplatePipelineName(templateJsonStringWithPipelinePlaceholder,
                    pipelineNameThatNeedsTransformation);
            LOG.info("Template - {}",templateJsonString);

            // Find all PLACEHOLDER_PATTERN in template json string
            Map<String, List<String>> placeholdersMap = findPlaceholdersWithPathsRecursively(templateJsonString);
            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);

            // get exact path in pipelineJson
            Map<String, String> pipelineExactPathMap = findExactPath(placeholdersMap, pipelineJson);

            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder, templateJsonPathList) -> {
                for (String templateJsonPath : templateJsonPathList) {
                    String pipelineExactJsonPath = pipelineExactPathMap.get(placeholder);

                    if(isJsonPath(pipelineExactJsonPath)) {
                        JsonNode pipelineNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelineJson).read(pipelineExactJsonPath);
                        // Json Path expression like "?(@.<node>)" seem to always return arrayNode even if it is an Object.
                        // example: $.pipeline.sink[?(@.opensearch)].opensearch.aws expression will always return array
                        if (pipelineExactJsonPath.contains(JSON_PATH_ARRAY_DISAMBIGUATOR_PATTERN) &&
                                pipelineNode.isArray() && pipelineNode.size() == 1) {
                            pipelineNode = pipelineNode.get(0);
                        }
                        replaceNode(templateRootNode, templateJsonPath, pipelineNode);
                    }else{ //in case it was a function call
                        JsonNode pipelineNode = objectMapper.valueToTree(pipelineExactJsonPath);
                        replaceNode(templateRootNode, templateJsonPath, pipelineNode);
                    }
                }
            });

            PipelinesDataFlowModel transformedPipelinesDataFlowModel = getTransformedPipelinesDataFlowModel(pipelineNameThatNeedsTransformation,
                    preTransformedPipelinesDataFlowModel,
                    templateRootNode,
                    subPipelineNames);
            return transformedPipelinesDataFlowModel;
        } catch (JsonProcessingException | TransformerException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkForSubPipelines(PipelinesDataFlowModel preTransformedPipelinesDataFlowModel,
                                      String pipelineNameThatNeedsTransformation,
                                      List<String> subPipelineNames) {
        Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
        PipelineModel transformationPipeline = pipelines.get(pipelineNameThatNeedsTransformation);

        List<SinkModel> sinks = transformationPipeline.getSinks();
        for(SinkModel sink : sinks){
            String pluginName = sink.getPluginName();
            if (pluginName.equals(SINK_SUBPIPELINE_PLUGIN_NAME)) {
                String subPipelineName = sink.getPluginSettings().get("name").toString();
                subPipelineNames.add(subPipelineName);
            }
        }
    }

    /**
     * Convert templateRootNode which contains the transformedJson to PipelinesDataFlowModel
     *
     * @param pipelineNameThatNeedsTransformation
     * @param preTransformedPipelinesDataFlowModel
     * @param templateRootNode                     - transformedJson Node.
     * @param subPipelineNames
     * @return PipelinesDataFlowModel - transformed model.
     * @throws JsonProcessingException
     */
    private PipelinesDataFlowModel getTransformedPipelinesDataFlowModel(String pipelineNameThatNeedsTransformation,
                                                                        PipelinesDataFlowModel preTransformedPipelinesDataFlowModel,
                                                                        JsonNode templateRootNode,
                                                                        List<String> subPipelineNames) throws JsonProcessingException {

        //update template json
        JsonNode transformedJsonNode = templateRootNode.get(TEMPLATE_PIPELINE_ROOT_STRING);
        String transformedJson = objectMapper.writeValueAsString(transformedJsonNode);
        LOG.info("{} pipeline has been transformed to :{}", pipelineNameThatNeedsTransformation, transformedJson);

        //convert TransformedJson to PipelineModel with the data from preTransformedDataFlowModel.
        //transform transformedJson to Map
        PipelinesDataFlowModel transformedSinglePipelineDataFlowModel = objectMapper.readValue(transformedJson, PipelinesDataFlowModel.class);
        Map<String, PipelineModel> transformedPipelines = transformedSinglePipelineDataFlowModel.getPipelines();

        Map<String, PipelineModel> pipelines = preTransformedPipelinesDataFlowModel.getPipelines();
        pipelines.forEach((pipelineName, pipeline) -> {
            if (!pipelineName.equals(pipelineNameThatNeedsTransformation)) {
                if(subPipelineNames.size()>0 && subPipelineNames.contains(pipelineName)){ //if there are subpipelines
                    try {
                        PipelineModel subPipeline = getSubModifiedPipeline(pipeline, pipelineNameThatNeedsTransformation);
                        transformedPipelines.put(pipelineName, subPipeline);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }else {
                    transformedPipelines.put(pipelineName, pipeline);
                }
            }
        });

        // version is not required here as it is already handled in parseStreamToPipelineDataFlowModel
        PipelinesDataFlowModel transformedPipelinesDataFlowModel = new PipelinesDataFlowModel(
                preTransformedPipelinesDataFlowModel.getPipelineExtensions(),
                transformedPipelines
        );
        String transformedPipelinesDataFlowModelJson = objectMapper.writeValueAsString(transformedPipelinesDataFlowModel);
        LOG.info("Transformed PipelinesDataFlowModel: {}", transformedPipelinesDataFlowModelJson);

        return transformedPipelinesDataFlowModel;
    }

    private PipelineModel getSubModifiedPipeline(PipelineModel pipeline,
                                         String pipelineNameThatNeedsTransformation) throws JsonProcessingException {
        String pipelineJson = objectMapper.writeValueAsString(pipeline);
        JsonNode pipelineNode = objectMapper.readTree(pipelineJson);
        JsonNode parentNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelineNode).read(SUBPIPELINE_PATH);

        //TODO - Dynamically detect the 2nd pipeline in the template of the transformed pipeline
        JsonNode newNode = new TextNode(pipelineNameThatNeedsTransformation +"-s3");
        ((ObjectNode) parentNode).replace("name", newNode);
        String subPipelineJson = objectMapper.writeValueAsString(pipelineNode);
        PipelineModel subPipeline = objectMapper.readValue(subPipelineJson, PipelineModel.class);

        return subPipeline;
    }

    private String replaceTemplatePipelineName(String templateJsonStringWithPipelinePlaceholder, String pipelineName) {
        return templateJsonStringWithPipelinePlaceholder.replaceAll(PIPELINE_NAME_PLACEHOLDER_REGEX, pipelineName);
    }

    /**
     * Recursively walks through the json to find the placeholder with a certain regEx pattern,
     * along the way keeps track of the path.
     *
     * @param json
     * @return Map<String, List < String>> , K:placeholder, V: list of jsonPath in templateJson
     * @throws IOException
     */
    private Map<String, List<String>> findPlaceholdersWithPathsRecursively(String json) throws IOException {

        JsonNode rootNode = objectMapper.readTree(json);
        Map<String, List<String>> placeholdersWithPaths = new HashMap<>();
        populateMapWithPlaceholderPaths(rootNode, "", placeholdersWithPaths);
        return placeholdersWithPaths;
    }

    private void populateMapWithPlaceholderPaths(JsonNode currentNode, String currentPath, Map<String, List<String>> placeholdersWithPaths) {
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
            Matcher matcher = PLACEHOLDER_PATTERN.matcher(placeHolderValue);
            if (matcher.find()) {
                if (!placeholdersWithPaths.containsKey(placeHolderValue)) {
                    List<String> paths = new ArrayList<>();
                    paths.add(currentPath);
                    placeholdersWithPaths.put(placeHolderValue, paths);
                } else {
                    List<String> existingPaths = placeholdersWithPaths.get(placeHolderValue);
                    existingPaths.add(currentPath);
                    placeholdersWithPaths.put(placeHolderValue, existingPaths);
                }
            }
        }
    }

    /**
     * Gets exact path - this is to avoid
     * getting array values(even though it might not be an array) given
     * a recursive expression like "$..<>"
     *
     * @param placeholdersMap
     * @return Map<String, String> K:jsonPath, V:exactPath
     * @throws IOException
     */
    private Map<String, String> findExactPath(Map<String, List<String>> placeholdersMap, String pipelineJson) throws IOException, TransformerException {
        Map<String, String> mapWithPaths = new HashMap<>();
        for (String genericPathPlaceholder : placeholdersMap.keySet()) {
            String placeHolderValue = getValueFromPlaceHolder(genericPathPlaceholder);

            String value = executeFunctionPlaceholder(placeHolderValue, pipelineJson);

        // Recursive pattern in json path is NOT allowed
            if (value!=null && value.contains(RECURSIVE_JSON_PATH_PATH)) {
                throw new TransformerException(format("Json path {} is not supported", value));
            }
            mapWithPaths.put(genericPathPlaceholder, value);
        }
        return mapWithPaths;
    }

    /**
     * Get value from the placeholder field.
     * Removes the brackets surrounding the value.
     *
     * @param placeholder
     * @return String - placeholder value.
     */
    private String getValueFromPlaceHolder(String placeholder) {
        // placeholder should be valid here as it is regEx matched in populateMapWithPlaceholderPaths
        return placeholder.substring(2, placeholder.length() - 2);
    }

    /**
     *
     * @param functionPlaceholderValue
     * @return String - value of the function executed
     */
    private String executeFunctionPlaceholder(String functionPlaceholderValue, String pipelineJson){
        Matcher functionMatcher = FUNCTION_CALL_PLACEHOLDER_PATTERN.matcher(functionPlaceholderValue);
        if (functionMatcher.find()) {
            String functionName = functionMatcher.group(1);
            String parameter = functionMatcher.group(2);
            try {
                String parameterValue = (String)parseParameter(parameter, pipelineJson);
                String value = (String) invokeMethod(functionName, String.class, parameterValue);
                return value;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return functionPlaceholderValue;
        }
    }

    private Object parseParameter(String parameter, String pipelineJson) {
        if(isJsonPath(parameter)){
            JsonNode pipelineNode = JsonPath.using(parseConfigWithJsonNode).parse(pipelineJson).read(parameter);
            if(pipelineNode==null){
                return null;
            }
            if(!pipelineNode.isValueNode()){
                throw new RuntimeException("parameter has to be a value node");
            }
            String nodeValue = pipelineNode.asText();
            return nodeValue;
        }
        return parameter;
    }

    /**
     * Check if the parameter passed is a json path or not.
     * @param parameter
     * @return boolean
     */
    private boolean isJsonPath(String parameter) {
        try {
            if (parameter == null){
                return false;
            }
            if(parameter.contains(JSON_PATH_IDENTIFIER)){
                JsonPath.compile(parameter);
                return true;
            }
            return false;
        } catch (IllegalArgumentException | PathNotFoundException e) {
            return false;
        }
    }

    /**
     * Calculate s3 folder scan depth for DocDB source pipeline
     * @param s3Prefix: s3 prefix defined in the source configuration
     * @return s3 folder scan depth
     */
    public String calculateDepth(String s3Prefix) {
        return Integer.toString(getDepth(s3Prefix, 4));
    }

    /**
     * Calculate s3 folder scan depth for RDS source pipeline
     * @param s3Prefix: s3 prefix defined in the source configuration
     * @return s3 folder scan depth
     */
    public String calculateDepthForRdsSource(String s3Prefix) {
        return Integer.toString(getDepth(s3Prefix, 3));
    }

    private int getDepth(String s3Prefix, int baseDepth) {
        if(s3Prefix == null){
            return baseDepth;
        }
        return s3Prefix.split("/").length + baseDepth;
    }

    public String getSourceCoordinationIdentifierEnvVariable(String s3Prefix){
        String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        if(s3Prefix == null){
            return envSourceCoordinationIdentifier;
        }
        return s3Prefix+"/"+envSourceCoordinationIdentifier;
    }

    /**
     * Get the include_prefix in s3 scan source. This is a function specific to RDS source.
     * @param s3Prefix: s3 prefix defined in the source configuration
     * @return the actual include_prefix
     */
    public String getIncludePrefixForRdsSource(String s3Prefix) {
        final String envSourceCoordinationIdentifier = System.getenv(SOURCE_COORDINATION_IDENTIFIER_ENVIRONMENT_VARIABLE);
        final String shortenedSourceIdentifier = envSourceCoordinationIdentifier != null ?
                IdentifierShortener.shortenIdentifier(envSourceCoordinationIdentifier, MAX_SOURCE_IDENTIFIER_LENGTH) : null;
        if (s3Prefix == null && envSourceCoordinationIdentifier == null) {
            return S3_BUFFER_PREFIX;
        } else if (s3Prefix == null) {
            return shortenedSourceIdentifier + S3_BUFFER_PREFIX;
        } else if (envSourceCoordinationIdentifier == null) {
            return s3Prefix + S3_BUFFER_PREFIX;
        }
        return s3Prefix + "/" + shortenedSourceIdentifier + S3_BUFFER_PREFIX;
    }

    public String getAccountIdFromRole(final String roleArn) {
        if (roleArn == null)
            return null;
        try {
            return Arn.fromString(roleArn).accountId().orElse(null);
        } catch (Exception e) {
            LOG.warn("Malformatted role ARN for dynamic transformation: {}", roleArn);
            return null;
        }
    }

    /**
     * Invokes a method dynamically on a given object.
     *
     * @param methodName    the name of the method to be invoked
     * @param parameterType the Class object representing the parameter type
     * @param arg           the parameter to be passed to the method
     * @return the result of the method invocation
     * @throws ReflectiveOperationException if the method cannot be invoked
     */
    public Object invokeMethod(String methodName, Class<?> parameterType, Object arg) throws ReflectiveOperationException {
        // Get the Class object
        Class<?> clazz = this.getClass();

        // Get the Method object for the specified method and parameter type
        Method method = clazz.getMethod(methodName, parameterType);

        // Invoke the method on the object with the given argument
        return method.invoke(this, arg);
    }


    /**
     * Replaces template node in the jsonPath with the node from
     * original json.
     *
     * @param root json root node
     * @param jsonPath json path
     * @param newNode new node to be repalces with
     */
    public void replaceNode(JsonNode root, String jsonPath, JsonNode newNode) {
        try {
            if (newNode == null) {
                LOG.info("Did not find jsonPath {}",jsonPath);
            }
            // Read the parent path of the target node
            String parentPath = jsonPath.substring(0, jsonPath.lastIndexOf('.'));
            String fieldName = jsonPath.substring(jsonPath.lastIndexOf('.') + 1);

            //Handle if fieldName is an array
            Pattern pattern = Pattern.compile(ARRAY_NODE_PATTERN);
            Matcher matcher = pattern.matcher(fieldName);

            // Find the parent node
            JsonNode parentNode = JsonPath.using(parseConfigWithJsonNode).parse(root).read(parentPath);

            // Replace the target field in the parent node
            if(matcher.find()){
                //Handle array
                String field = matcher.group(1);
                int index = Integer.parseInt(matcher.group(2));
                JsonNode arrayNodeResult = JsonPath.using(parseConfigWithJsonNode).parse(root).read(parentPath+"."+field);
                if (!(arrayNodeResult instanceof ArrayNode)){
                    throw new RuntimeException("Json path is of array type, but parsed result is not arrayNode");
                }
                ArrayNode arrayNode = (ArrayNode) arrayNodeResult;
                // Replace the element in the array
                arrayNode.set(index, newNode);
            } else if (parentNode != null && parentNode instanceof ObjectNode) {
                ((ObjectNode) parentNode).replace(fieldName, newNode);
            } else {
                LOG.error("Path does not point to object node");
                throw new IllegalArgumentException("Path does not point to object node");
            }
        } catch (PathNotFoundException e) {
            LOG.error("JsonPath {} not found", jsonPath);
            throw new PathNotFoundException(format("JsonPath {} not found", jsonPath));
        }
    }
}
