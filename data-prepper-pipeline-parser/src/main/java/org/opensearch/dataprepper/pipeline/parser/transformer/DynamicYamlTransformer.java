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
import com.jayway.jsonpath.JsonPathException;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class DynamicYamlTransformer implements PipelineConfigurationTransformer {

    //.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ReadContext readObjectContextPipeline = null;
    private ReadContext readPathsContextPipeline = null;

    private ReadContext readObjectContextTemplate = null;
    private ReadContext readPathsContextTemplate = null;
    private DocumentContext documentContextTemplate = null;  //write the transformed pipelineDataflowModel
    String pipelinesDataFlowModelJsonString;
    String templateDataFlowModelJsonString;

    ParseContext mainParseContext = JsonPath.using(Configuration.builder()
            .jsonProvider(new JacksonJsonProvider())
            .mappingProvider(new JacksonMappingProvider())
            .options(Option.ALWAYS_RETURN_LIST)
            .build());

    private final PipelinesDataFlowModel pipelinesDataFlowModel;
    private final PipelinesDataFlowModel templateDataFlowModel;
    //TODO
    //change delimiter
    private final Pattern placeholderPattern = Pattern.compile("\\{\\{(.+?)}}");

    public DynamicYamlTransformer(PipelinesDataFlowModel pipelinesDataFlowModel,
                                  PipelinesDataFlowModel templateDataFlowModel) {

        this.pipelinesDataFlowModel = pipelinesDataFlowModel;
        this.templateDataFlowModel = templateDataFlowModel;

        try {
            this.pipelinesDataFlowModelJsonString = objectMapper.writeValueAsString(pipelinesDataFlowModel);
            this.templateDataFlowModelJsonString = objectMapper.writeValueAsString(templateDataFlowModel);

            this.readPathsContextPipeline = mainParseContext.parse(pipelinesDataFlowModelJsonString);

            this.readObjectContextTemplate = mainParseContext.parse(templateDataFlowModelJsonString);
            this.documentContextTemplate = mainParseContext.parse(templateDataFlowModelJsonString);
        } catch (final JsonPathException e) {
            throw new IllegalArgumentException("Unable to parse json string PipelinesDataFlowModel from ParseContext", e);
        } catch (final JsonProcessingException e) {
            throw new IllegalArgumentException("PipelinesDataFlowModel cannot be parsed to json string", e);
        }

    }

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

    @Override
    public PipelinesDataFlowModel transformConfiguration(PipelinesDataFlowModel pipelinesDataFlowModel,
                                                         String templateJsonString) {
        //find all placeholderPattern

        // Define a regex pattern to find all placeholders
        Pattern pattern = Pattern.compile("\\{\\{(.+?)}}");
        Matcher matcher = pattern.matcher(pipelinesDataFlowModelJsonString);

        while (matcher.find()) {
            // Extracted placeholder value
            String jsonPathPattern =  matcher.group(1);
            System.out.println(jsonPathPattern);
            String datafromOriginal = readPathsContextPipeline.read(jsonPathPattern);

            System.out.println(datafromOriginal);
        }
        //replace the placeholder value with original readcontext.read(value)

        //write to documentContext

        return null;
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
