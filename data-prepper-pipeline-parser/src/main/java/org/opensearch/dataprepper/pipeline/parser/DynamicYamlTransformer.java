package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class DynamicYamlTransformer implements PipelineYamlTransformer {

        private final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());

        @Override
        public String transformYaml(String originalYaml, String templateYaml) {
            try {
                JsonNode originalRoot = yamlMapper.readTree(originalYaml);
                JsonNode templateRoot = yamlMapper.readTree(templateYaml);

                // Dynamically parse originalRoot to apply transformations.
                updateTemplateWithOriginal(templateRoot, originalRoot, "");

                return yamlMapper.writeValueAsString(templateRoot);
            } catch (IOException e) {
                throw new RuntimeException("Error processing YAML", e);
            }
        }

        private void updateTemplateWithOriginal(JsonNode templateRoot, JsonNode originalRoot, String currentPath) {
            if (originalRoot.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = originalRoot.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String newPath = currentPath.isEmpty() ? field.getKey() : currentPath + "." + field.getKey();
                    updateTemplateWithOriginal(templateRoot, field.getValue(), newPath);
                }
            } else {
                substituteNode(templateRoot, currentPath, originalRoot);
            }
        }

        private void substituteNode(JsonNode templateRoot, String path, JsonNode replacementNode) {
            String[] parts = path.split("\\.");
            JsonNode currentNode = templateRoot;
            for (int i = 0; i < parts.length - 1; i++) {
                if (!currentNode.has(parts[i])) {
                    return; // Path not found in template, no substitution needed.
                }
                currentNode = currentNode.path(parts[i]);
            }
            if (currentNode instanceof ObjectNode) {
                ((ObjectNode) currentNode).set(parts[parts.length - 1], replacementNode);
            }
        }
    }
