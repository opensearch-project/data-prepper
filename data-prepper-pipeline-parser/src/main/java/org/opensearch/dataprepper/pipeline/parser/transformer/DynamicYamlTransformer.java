package org.opensearch.dataprepper.pipeline.parser.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import static com.jayway.jsonpath.JsonPath.read;
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineTemplateModel;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

public class DynamicYamlTransformer implements PipelineConfigurationTransformer {

    //.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
    private final ObjectMapper objectMapper = new ObjectMapper();

    Pattern placeholderPattern = Pattern.compile("\\{\\{\\s*(.+?)\\s*}}");
    ParseContext mainParseContext = JsonPath.using(Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.ALWAYS_RETURN_LIST)
            .build());


//TODO
//    DynamicYamlTransformer(templateProvider(interface))
//
//    /**
//     * Recursively look for text nodes that match a specific placeholder pattern (in this case, {{placeholder}}).
//     * When it finds a match, attempt to replace the placeholder with the corresponding value
//     * from another JSON tree (originalRoot).
//     *
//     * @param originalYaml The original YAML configuration.
//     * @param templateYaml The template YAML with placeholders.
//     * @return
//     */
//    @Override
//    public String transformYaml(String originalYaml, String templateYaml) {
////        pipedataflowmodel -> yaml
//        //convert pipelinesDataFlowModel to yaml.
//        try {
//            JsonNode originalRoot = objectMapper.readTree(originalYaml);
//            JsonNode templateRoot = objectMapper.readTree(templateYaml);
//
////            printNodeTypes(originalRoot, "");
//
////            replacePlaceholders(templateRoot, originalRoot);
//
//            return objectMapper.writeValueAsString(templateRoot);
//        } catch (IOException e) {
//            throw new RuntimeException("Error processing YAML", e);
//        }
//    }

    //TODO
    // address pipeline_configurations and version
    // address sub-piplines
    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                         PipelineTemplateModel templateModel) {

        try {
            String pipelineNameThatNeedsTransformation;
            String templateJsonString = objectMapper.writeValueAsString(templateModel);
            String pipelinesDataFlowModelJsonString = objectMapper.writeValueAsString(pipelinesDataFlowModel);
            ReadContext readPathsContextPipeline = mainParseContext.parse(pipelinesDataFlowModelJsonString);

            ReadContext readObjectContextTemplate = mainParseContext.parse(templateJsonString);
            DocumentContext documentContextTemplate = mainParseContext.parse(templateJsonString);

            // Define a regex pattern to find all placeholder
            Matcher templateMatcher = placeholderPattern.matcher(templateJsonString);


//        List<?> allValues = JsonPath.read(templateJsonString, "$..*");
//        // Filter values by checking if they are instances of String and match the regex pattern
//        List<String> placeholders = allValues.stream()
//                    .filter(String.class::isInstance) // Ensure only Strings are processed
//                    .map(String.class::cast) // Cast to String safely
//                    .filter(value -> placeholderPattern.matcher(value).find()) // Use find() to locate the pattern anywhere within the string
//                    .collect(Collectors.toList());

            //find all placeholderPattern in template json string
            // K:placeholder , V:jsonPath
            Map<String,String> placeholdersMap = findPlaceholdersWithPaths(templateJsonString);

            JsonNode templateRootNode = objectMapper.readTree(templateJsonString);
            JsonNode pipelineRootNode = objectMapper.readTree(pipelinesDataFlowModelJsonString);

            //replace placeholder with actual value in the template context
            placeholdersMap.forEach((placeholder,templateJsonPath) ->{
                String pipelineJsonPath = getValueFromPlaceHolder(placeholder);

                // get value from pipelineJsonPath
                Map<String,Object> pipelineValue = readPathsContextPipeline.read(pipelineJsonPath);

                // replace the placeholder data with the pipelineValue at templateJsonPath
//                replaceTemplatePlaceholders();
                //use JsonNode to replace.

//                replaceNode(templateRootNode, pipelineJsonPath, newNode);
//                JsonNode pipelineNode = JsonPath.read(pipelineJsonPath, pipelineJsonPath);
//                templateJsonString.replace(placeholder,pipelineNode.toString());

            });


        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            //error when
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

    private String getValueFromPlaceHolder(String placeholder){
        if (placeholder.length()< 4){
            //TODO
            //invalid placeholder
            throw new RuntimeException();
        }

        //remove the first 2 and last 2 characters.
        return placeholder.substring(2,placeholder.length()-2);
    }

    //TODO - old way; recursive
    //
//    private void printNodeTypes(JsonNode node, String path) {
//        if (node.isObject()) {
//            System.out.println(path + " (Object)");
//            node.fields().forEachRemaining(field -> printNodeTypes(field.getValue(), path + "." + field.getKey()));
//        } else if (node.isArray()) {
//            System.out.println(path + " (Array)");
//            for (int i = 0; i < node.size(); i++) {
//                printNodeTypes(node.get(i), path + "[" + i + "]");
//            }
//        } else if (node.isTextual()) {
//            System.out.println(path + " (String)");
//        } else if (node.isNumber()) {
//            System.out.println(path + " (Number)");
//        } else if (node.isBoolean()) {
//            System.out.println(path + " (Boolean)");
//        } else if (node.isNull()) {
//            System.out.println(path + " (Null)");
//        } else {
//            System.out.println(path + " (Unknown Type)");
//        }
//    }
//
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
