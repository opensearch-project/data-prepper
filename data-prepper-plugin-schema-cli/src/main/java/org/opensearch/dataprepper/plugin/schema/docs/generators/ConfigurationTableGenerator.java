package org.opensearch.dataprepper.plugin.schema.docs.generators;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Generates markdown tables for plugin configuration options.
 */
public class ConfigurationTableGenerator {

    /**
     * Generate a markdown table documenting the configuration options.
     *
     * @param schema JSON schema containing configuration properties
     * @return Generated markdown table
     */
    public String generate(final JsonNode schema) {
        final List<String> sections = new ArrayList<>();

        // Add description if available
        if (schema.has("description")) {
            sections.add(schema.get("description").asText());
        }

        sections.add("The following table describes options you can use with this plugin.");
        sections.add("");

        // Add table header
        sections.add("Option | Required | Type | Description");
        sections.add(":--- | :--- |:--- | :---");

        // Get properties node
        final JsonNode properties = schema.get("properties");
        if (properties == null || !properties.isObject()) {
            return String.join("\n", sections);
        }

        // Get required properties
        final List<String> required = new ArrayList<>();
        final JsonNode requiredNode = schema.get("required");
        if (requiredNode != null && requiredNode.isArray()) {
            requiredNode.forEach(node -> required.add(node.asText()));
        }

        // Process each property
        final Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> field = fields.next();
            final String propertyRow = generatePropertyRow(
                field.getKey(),
                field.getValue(),
                required.contains(field.getKey())
            );
            sections.add(propertyRow);
        }

        return String.join("\n", sections);
    }

    private String generatePropertyRow(final String name,
                                    final JsonNode property,
                                    final boolean isRequired) {
        final String type = getPropertyType(property);
        final String description = formatDescription(property);

        return String.format("`%s` | %s | %s | %s",
            name,
            isRequired ? "Yes" : "No",
            formatType(type),
            description);
    }

    private String getPropertyType(final JsonNode property) {
        final JsonNode typeNode = property.get("type");
        if (typeNode == null) {
            return "";
        }

        // Handle multiple types
        if (typeNode.isArray()) {
            final List<String> types = new ArrayList<>();
            typeNode.forEach(node -> types.add(formatTypeName(node.asText())));
            return String.join(" or ", types);
        }

        final String type = typeNode.asText();

        // Handle array types
        if ("array".equals(type)) {
            final JsonNode items = property.get("items");
            if (items != null && items.has("type")) {
                return String.format("List<%s>", formatTypeName(items.get("type").asText()));
            }
            return "List";
        }

        return formatTypeName(type);
    }

    private String formatTypeName(final String type) {
        // Convert some types to more readable formats
        switch (type.toLowerCase()) {
            case "integer":
                return "Integer";
            case "boolean":
                return "Boolean";
            case "string":
                return "String";
            case "object":
                return "Map";
            default:
                return type;
        }
    }

    private String formatType(final String type) {
        // Don't wrap simple types in backticks
        if (type.equals("String") || type.equals("Integer") || type.equals("Boolean") ||
            type.equals("Map") || type.startsWith("List<")) {
            return type;
        }
        // Wrap complex/custom types in backticks
        return String.format("`%s`", type);
    }

    private String formatDescription(final JsonNode property) {
        final StringBuilder description = new StringBuilder();

        // Add main description
        if (property.has("description")) {
            String desc = property.get("description").asText()
                // Replace HTML code tags with backticks
                .replaceAll("<code>([^<]+)</code>", "`$1`");
            description.append(desc);
        }

        // Add default value if present
        if (property.has("default")) {
            final JsonNode defaultValue = property.get("default");
            if (description.length() > 0) {
                description.append(" ");
            }
            description.append("Default is ");
            if (defaultValue.isTextual()) {
                description.append("`\"").append(defaultValue.asText()).append("\"`");
            } else if (defaultValue.isNull()) {
                description.append("`null`");
            } else if (defaultValue.isArray()) {
                description.append("`[]`");
            } else if (defaultValue.isObject()) {
                description.append("`{}`");
            } else {
                description.append("`").append(defaultValue.toString()).append("`");
            }
            description.append(".");
        }

        // Add allowed values if present
        if (property.has("enum")) {
            if (description.length() > 0) {
                description.append(" ");
            }
            description.append("Allowed values: ");
            final List<String> values = new ArrayList<>();
            property.get("enum").forEach(value ->
                values.add(String.format("`%s`", value.asText()))
            );
            description.append(String.join(", ", values));
            description.append(".");
        }

        // Add deprecated notice if applicable
        if (property.has("deprecated")) {
            if (description.length() > 0) {
                description.append(" ");
            }
            description.append("**Deprecated");
            final String since = property.get("deprecated").asText();
            if (!since.isEmpty()) {
                description.append(" since ").append(since);
            }
            description.append(".**");
        }

        // Handle empty description
        if (description.length() == 0) {
            return "No description available.";
        }

        return description.toString();
    }
}