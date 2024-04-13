package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.rule.RuleEvaluator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicConfigTransformer implements PipelineConfigurationTransformer {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RuleEvaluator ruleEvaluator;

    Pattern placeholderPattern = Pattern.compile("\\{\\{\\s*(.+?)\\s*}}");

    // Configuration necessary for JsonPath to work with Jackson
    ParseContext mainParseContext = JsonPath.using(Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .build());

    public DynamicConfigTransformer(RuleEvaluator ruleEvaluator) {
        this.ruleEvaluator = ruleEvaluator;
    }

    //TODO
    // address pipeline_configurations and version
    // address sub-piplines
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                         PipelineTemplateModel templateModel) {
        if (!ruleEvaluator.isTransformationNeeded(pipelinesDataFlowModel)) {
            return pipelinesDataFlowModel;
        }

        try {
            String pipelineNameThatNeedsTransformation;
            String templateJsonString = objectMapper.writeValueAsString(templateModel);
            String pipelinesDataFlowModelJsonString = objectMapper.writeValueAsString(pipelinesDataFlowModel);
            ReadContext pipelineReadContext = mainParseContext.parse(pipelinesDataFlowModelJsonString);

            ReadContext templateReadContext = mainParseContext.parse(templateJsonString);
            //TODO needed?
//            DocumentContext documentContextTemplate = mainParseContext.parse(templateJsonString);

//        List<?> allValues = JsonPath.read(templateJsonString, "$..*");
//        // Filter values by checking if they are instances of String and match the regex pattern
//        List<String> placeholders = allValues.stream()
//                    .filter(String.class::isInstance) // Ensure only Strings are processed
//                    .map(String.class::cast) // Cast to String safely
//                    .filter(value -> placeholderPattern.matcher(value).find()) // Use find() to locate the pattern anywhere within the string
//                    .collect(Collectors.toList());

            //find all placeholderPattern in template json string
            // K:placeholder , V:jsonPath
            Map<String, String> placeholdersMap = findPlaceholdersWithPaths(templateJsonString);

            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);
//            JsonNode pipelineRootNode = objectMapper.readTree(pipelinesDataFlowModelJsonString);

            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder, templateJsonPath) -> {
                String pipelineJsonPath = getValueFromPlaceHolder(placeholder);

                // replace the placeholder data with the pipelineValue at templateJsonPath
//                replaceTemplatePlaceholders();
                //use JsonNode to replace.
                JsonNode pipelineNode = pipelineReadContext.read(pipelineJsonPath, JsonNode.class);

                //replace pipelineNode in the template
                replaceNode(templateRootNode, templateReadContext, templateJsonPath, pipelineNode);
            });

            //update template json
            String transformedJson = objectMapper.writeValueAsString(templateRootNode);

            //transform transformedJson to pipelinesModels

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
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

    public void replaceNode(JsonNode root, ReadContext rootContext, String jsonPath, JsonNode newNode) {
        try {
            // Read the parent path of the target node
            String parentPath = jsonPath.substring(0, jsonPath.lastIndexOf('.'));
            String fieldName = jsonPath.substring(jsonPath.lastIndexOf('.') + 1);

            // Find the parent node
            JsonNode parentNode = rootContext.read(parentPath, JsonNode.class);
//            JsonNode parentNode = JsonPath.using(configuration).parse(root).read(parentPath);

            // Replace the target field in the parent node
            if (parentNode instanceof ObjectNode) {
                ((ObjectNode) parentNode).set(fieldName, newNode);
            } else {
                throw new IllegalArgumentException("Path does not point to an object node");
            }
        } catch (PathNotFoundException e) {
            throw new PathNotFoundException(e);
        }
    }

//    private void replacePlaceholders(JsonNode node, JsonNode originalRoot) {
//        //go to every node in template, look for placeholder.
//        //get placeholder and read the path from the originalRoot.
//        if (node.isObject()) {
//            processObjectNode((ObjectNode) node, originalRoot);
//        } else if (node.isArray()) {
//            node.forEach(child -> replacePlaceholders(child, originalRoot));
//        }
//    }
//
//    private void processObjectNode(ObjectNode objectNode, JsonNode originalRoot) {
//        Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
//        while (iterator.hasNext()) {
//            Map.Entry<String, JsonNode> entry = iterator.next();
//            JsonNode currentNode = entry.getValue();
//            //TODO add number, boolean, etc?
//            if (currentNode.isTextual()) {
//                replaceTextNode(objectNode, entry.getKey(), currentNode.textValue(), originalRoot);
//            } else {
//                replacePlaceholders(currentNode, originalRoot);
//            }
//        }
//    }
//
//    private void replaceTextNode(ObjectNode objectNode, String key, String value, JsonNode originalRoot) {
//        Matcher matcher = placeholderPattern.matcher(value);
//        if (matcher.find()) {
//            String placeholderPath = matcher.group(1);
//            JsonNode replacementNode = getNodeByPath(originalRoot, placeholderPath);
//            if (replacementNode != null) {
//                objectNode.set(key, replacementNode);
//            }
//        }
//    }
//
//    private JsonNode getNodeByPath(JsonNode rootNode, String path) {
//        JsonNode currentNode = rootNode;
//        for (String part : path.split("\\.")) {
//            currentNode = currentNode.get(part);
//            if (currentNode == null) {
//                return null;
//            }
//        }
//        return currentNode;
//    }

//TODO - JSONPointer usage

//
//    // Impl 2
//    public void transformYaml(File originalFile, File templateFile, File outputFile) throws IOException {
//        // Parse the original and template YAMLs
//        JsonNode originalRoot = objectMapper.readTree(originalFile);
//        JsonNode templateRoot = objectMapper.readTree(templateFile);
//
//        // Recursively replace placeholders in the template
//        replacePlaceholders(templateRoot, originalRoot);
//
//        // Write the transformed YAML to the output file
//        objectMapper.writeValue(outputFile, templateRoot);
//    }
//
//    private void replacePlaceholders(JsonNode templateNode, JsonNode originalRoot) {
//        if (templateNode.isObject()) {
//            Iterator<Map.Entry<String, JsonNode>> fields = templateNode.fields();
//            while (fields.hasNext()) {
//                Map.Entry<String, JsonNode> field = fields.next();
//                if (field.getValue().isTextual()) {
//                    String placeholder = field.getValue().asText();
//                    JsonNode value = resolvePlaceholder(placeholder, originalRoot);
//                    if (value != null) {
//                        ((ObjectNode) templateNode).set(field.getKey(), value);
//                    }
//                } else {
//                    replacePlaceholders(field.getValue(), originalRoot);
//                }
//            }
//        }
//    }
//
//    private JsonNode resolvePlaceholder(String placeholder, JsonNode originalRoot) {
//        if (!placeholder.startsWith("{{") || !placeholder.endsWith("}}")) {
//            return null; // Not a placeholder
//        }
//        String path = placeholder.substring(2, placeholder.length() - 2); // Remove {{ and }}
//
//        //TODO check if JSON Path is better?
//        return originalRoot.at(pathToJsonPointer(path));
//    }
//
//    private String pathToJsonPointer(String path) {
//        // Convert a path like "source.documentdb.hostname" to a JSON Pointer "/source/documentdb/hostname"
//        return "/" + path.replace(".", "/");
//    }


}
