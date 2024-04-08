package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


public class SimpleYamlTransformer implements PipelineYamlTransformer{

    private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
    private final String delimiter = "{{";

    public void transformYaml(File originalFile, File templateFile, File outputFile) throws IOException {
        // Parse the original and template YAMLs
        JsonNode originalRoot = yamlMapper.readTree(originalFile);
        JsonNode templateRoot = yamlMapper.readTree(templateFile);

        // Recursively replace placeholders in the template
        replacePlaceholders(templateRoot, originalRoot);

        // Write the transformed YAML to the output file
        yamlMapper.writeValue(outputFile, templateRoot);
    }

    private void replacePlaceholders(JsonNode templateNode, JsonNode originalRoot) {
        if (templateNode.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = templateNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getValue().isTextual()) {
                    String placeholder = field.getValue().asText();
                    JsonNode value = resolvePlaceholder(placeholder, originalRoot);
                    if (value != null) {
                        ((ObjectNode) templateNode).set(field.getKey(), value);
                    }
                } else {
                    replacePlaceholders(field.getValue(), originalRoot);
                }
            }
        }
    }

    private JsonNode resolvePlaceholder(String placeholder, JsonNode originalRoot) {
        if (!placeholder.startsWith("{{") || !placeholder.endsWith("}}")) {
            return null; // Not a placeholder
        }
        String path = placeholder.substring(2, placeholder.length() - 2); // Remove {{ and }}
        return originalRoot.at(pathToJsonPointer(path));
    }

    private String pathToJsonPointer(String path) {
        // Convert a path like "source.documentdb.hostname" to a JSON Pointer "/source/documentdb/hostname"
        return "/" + path.replace(".", "/");
    }

    @Override
    public String transformYaml(String originalYaml, String templateYaml) {
        return null;
    }

// TODO
//    public static void main(String[] args) throws IOException {
//        File originalFile = new File("path/to/original.yaml");
//        File templateFile = new File("path/to/template.yaml");
//        File outputFile = new File("path/to/output.yaml");
//
//        new YamlTransformer().transformYaml(originalFile, templateFile, outputFile);
//        System.out.println("Transformation completed. Check the output file.");
//    }
}
