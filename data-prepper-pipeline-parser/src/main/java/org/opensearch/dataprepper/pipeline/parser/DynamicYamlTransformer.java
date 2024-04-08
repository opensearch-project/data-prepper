package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicYamlTransformer implements PipelineYamlTransformer {

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final Pattern placeholderPattern = Pattern.compile("\\{\\{(.+?)}}");

    /**
     * Recursively look for text nodes that match a specific placeholder pattern (in this case, {{placeholder}}).
     * When it finds a match, attempt to replace the placeholder with the corresponding value
     * from another JSON tree (originalRoot).
     *
     * @param originalYaml The original YAML configuration.
     * @param templateYaml The template YAML with placeholders.
     * @return
     */
    @Override
    public String transformYaml(String originalYaml, String templateYaml) {
        try {
            JsonNode originalRoot = yamlMapper.readTree(originalYaml);
            JsonNode templateRoot = yamlMapper.readTree(templateYaml);

//            printNodeTypes(originalRoot, "");

            replacePlaceholders(templateRoot, originalRoot);

            return yamlMapper.writeValueAsString(templateRoot);
        } catch (IOException e) {
            throw new RuntimeException("Error processing YAML", e);
        }
    }

    private void printNodeTypes(JsonNode node, String path) {
        if (node.isObject()) {
            System.out.println(path + " (Object)");
            node.fields().forEachRemaining(field -> printNodeTypes(field.getValue(), path + "." + field.getKey()));
        } else if (node.isArray()) {
            System.out.println(path + " (Array)");
            for (int i = 0; i < node.size(); i++) {
                printNodeTypes(node.get(i), path + "[" + i + "]");
            }
        } else if (node.isTextual()) {
            System.out.println(path + " (String)");
        } else if (node.isNumber()) {
            System.out.println(path + " (Number)");
        } else if (node.isBoolean()) {
            System.out.println(path + " (Boolean)");
        } else if (node.isNull()) {
            System.out.println(path + " (Null)");
        } else {
            System.out.println(path + " (Unknown Type)");
        }
    }

    private void replacePlaceholders(JsonNode node, JsonNode originalRoot) {
        if (node.isObject()) {
            processObjectNode((ObjectNode) node, originalRoot);
        } else if (node.isArray()) {
            node.forEach(child -> replacePlaceholders(child, originalRoot));
        }
    }

    private void processObjectNode(ObjectNode objectNode, JsonNode originalRoot) {
        Iterator<Map.Entry<String, JsonNode>> iterator = objectNode.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = iterator.next();
            JsonNode currentNode = entry.getValue();
            //TODO add number, boolean, etc?
            if (currentNode.isTextual()) {
                replaceTextNode(objectNode, entry.getKey(), currentNode.textValue(), originalRoot);
            } else {
                replacePlaceholders(currentNode, originalRoot);
            }
        }
    }

    private void replaceTextNode(ObjectNode objectNode, String key, String value, JsonNode originalRoot) {
        Matcher matcher = placeholderPattern.matcher(value);
        if (matcher.find()) {
            String placeholderPath = matcher.group(1);
            JsonNode replacementNode = getNodeByPath(originalRoot, placeholderPath);
            if (replacementNode != null) {
                objectNode.set(key, replacementNode);
            }
        }
    }

    private JsonNode getNodeByPath(JsonNode rootNode, String path) {
        JsonNode currentNode = rootNode;
        for (String part : path.split("\\.")) {
            currentNode = currentNode.get(part);
            if (currentNode == null) {
                return null;
            }
        }
        return currentNode;
    }
}
