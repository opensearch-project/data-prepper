package org.opensearch.dataprepper.plugins.processor.ocsf;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OcsfSchemaMapper {
    private static final Logger LOG = LoggerFactory.getLogger(OcsfSchemaMapper.class);
    private static final String DEFAULT_SCHEMA_VERSION = "1.0.0-rc.2";
    private final JSONObject schema;

    public OcsfSchemaMapper(String schemaPath) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(schemaPath)));
            schema = new JSONObject(content);
            validateSchema();
        } catch (Exception e) {
            throw new InvalidPluginConfigurationException("Failed to load schema from " + schemaPath, e);
        }
    }

    private void validateSchema() {
        if (!schema.has("mapping_options")) {
            throw new InvalidPluginConfigurationException("Schema must contain 'mapping_options' section");
        }
        if (!schema.has("mapping_selector")) {
            throw new InvalidPluginConfigurationException("Schema must contain 'mapping_selector' section");
        }
    }

    public Map<String, Object> mapToOcsf(Map<String, Object> sourceData) {
        try {
            String mappingType = selectMapping(sourceData);
            LOG.debug("Selected mapping type: {}", mappingType);

            JSONObject mappingOptions = schema.getJSONObject("mapping_options")
                    .getJSONObject(mappingType);

            Map<String, Object> ocsfData = new HashMap<>();

            applyDirectMappings(sourceData, mappingOptions.getJSONArray("direct_mappings"), ocsfData);
            applyTransformations(sourceData, mappingOptions.getJSONArray("transformations"), ocsfData);

            return ocsfData;
        } catch (Exception e) {
            LOG.error("Error mapping to OCSF: {}", e.getMessage());
            throw new RuntimeException("Failed to map data to OCSF", e);
        }
    }

    private String selectMapping(Map<String, Object> sourceData) throws JSONException {
        JSONObject selector = schema.getJSONObject("mapping_selector");
        JSONArray cases = selector.getJSONArray("cases");

        for (int i = 0; i < cases.length(); i++) {
            JSONObject caseObj = cases.getJSONObject(i);
            if (evaluateComplexCondition(caseObj.getJSONObject("condition"), sourceData)) {
                return caseObj.getString("selection");
            }
        }

        return selector.getString("default_selection");
    }

    private void applyDirectMappings(Map<String, Object> sourceData,
                                     JSONArray directMappings,
                                     Map<String, Object> ocsfData) throws JSONException {
        for (int i = 0; i < directMappings.length(); i++) {
            JSONObject mapping = directMappings.getJSONObject(i);
            String source = mapping.getString("source");
            String destination = mapping.getString("destination");
            String type = mapping.getString("type");

            if (source.contains("[?(@.Name==")) {
                handleArrayMapping(sourceData, source, destination, type, mapping, ocsfData);
            } else {
                Object sourceValue = getNestedValue(sourceData, source);
                if (sourceValue != null) {
                    Object value = convertValue(sourceValue, type, mapping);
                    if (value != null) {
                        setNestedValue(ocsfData, destination, value);
                    }
                }
            }
        }
    }

    private void handleArrayMapping(Map<String, Object> sourceData,
                                    String sourcePath,
                                    String destPath,
                                    String type,
                                    JSONObject mapping,
                                    Map<String, Object> ocsfData) {
        try {
            Object value = getArrayValue(sourceData, sourcePath);
            if (value != null) {
                Object convertedValue = convertValue(value, type, mapping);
                if (convertedValue != null) {
                    setNestedValue(ocsfData, destPath, convertedValue);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to handle array mapping for path: {} - {}", sourcePath, e.getMessage());
            LOG.debug("Full stack trace:", e);
        }
    }

    private Object getNestedValue(Map<String, Object> data, String path) {
        String[] parts = path.split("\\.");
        Object current = data;

        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
                if (current == null) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return current;
    }

    private Object getArrayValue(Map<String, Object> data, String path) {
        if (path.contains("[?(@.Name==")) {
            String arrayPath = path.substring(0, path.indexOf('['));
            Object arrayValue = getNestedValue(data, arrayPath);

            if (arrayValue instanceof List) {
                List<?> sourceList = (List<?>) arrayValue;
                String nameToMatch = path.substring(
                        path.indexOf("'") + 1,
                        path.lastIndexOf("'")
                );

                for (Object item : sourceList) {
                    if (item instanceof Map) {
                        Map<?, ?> itemMap = (Map<?, ?>) item;
                        if (nameToMatch.equals(itemMap.get("Name"))) {
                            if (path.endsWith(".Value")) {
                                return itemMap.get("Value");
                            }
                            return itemMap;
                        }
                    }
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void setNestedValue(Map<String, Object> data, String path, Object value) {
        String[] parts = path.split("\\.");
        Map<String, Object> current = data;

        for (int i = 0; i < parts.length - 1; i++) {
            current = (Map<String, Object>) current.computeIfAbsent(parts[i],
                    k -> new HashMap<String, Object>());
        }

        String lastPart = parts[parts.length - 1];
        if (lastPart.endsWith("[]")) {
            String arrayPath = lastPart.substring(0, lastPart.length() - 2);
            List<Object> list = (List<Object>) current.computeIfAbsent(arrayPath,
                    k -> new ArrayList<>());
            if (value instanceof List) {
                list.addAll((List<?>) value);
            } else {
                list.add(value);
            }
        } else {
            current.put(lastPart, value);
        }
    }

    private void applyTransformations(Map<String, Object> sourceData,
                                      JSONArray transformations,
                                      Map<String, Object> ocsfData) throws JSONException {
        for (int i = 0; i < transformations.length(); i++) {
            JSONObject transform = transformations.getJSONObject(i);
            String transformationType = transform.getString("transformation");

            switch (transformationType) {
                case "literal":
                    applyLiteralTransformation(transform, ocsfData);
                    break;
                case "enum":
                    applyEnumTransformation(transform, sourceData, ocsfData);
                    break;
                case "conditional":
                    applyConditionalTransformation(transform, sourceData, ocsfData);
                    break;
                case "variable":
                    applyVariableTransformation(transform, ocsfData);
                    break;
                default:
                    LOG.warn("Unsupported transformation type: {}", transformationType);
            }
        }
    }

    private void applyLiteralTransformation(JSONObject transform, Map<String, Object> ocsfData) throws JSONException {
        String target = transform.getString("target");
        Object value = transform.get("value");
        String type = transform.getString("type");

        Object convertedValue = convertValue(value, type, transform);
        if (convertedValue != null) {
            setNestedValue(ocsfData, target, convertedValue);
        }
    }

    private void applyEnumTransformation(JSONObject transform, Map<String, Object> sourceData, Map<String, Object> ocsfData) throws JSONException {
        String source = transform.getString("source");
        String target = transform.getString("target");
        String type = transform.getString("type");
        boolean ignoreCase = transform.optBoolean("ignoreCase", false);

        Object sourceValue = getNestedValue(sourceData, source);
        if (sourceValue != null) {
            JSONArray cases = transform.getJSONArray("cases");

            for (int i = 0; i < cases.length(); i++) {
                JSONObject caseObj = cases.getJSONObject(i);
                String fromValue = caseObj.getString("from");

                if ((ignoreCase && String.valueOf(sourceValue).equalsIgnoreCase(fromValue)) ||
                        (!ignoreCase && String.valueOf(sourceValue).equals(fromValue))) {
                    Object value = convertValue(caseObj.get("to"), type, transform);
                    if (value != null) {
                        setNestedValue(ocsfData, target, value);
                    }
                    return;
                }
            }

            if (transform.has("default")) {
                Object defaultValue = convertValue(transform.get("default"), type, transform);
                if (defaultValue != null) {
                    setNestedValue(ocsfData, target, defaultValue);
                }
            }
        }
    }

    private void applyConditionalTransformation(JSONObject transform,
                                                Map<String, Object> sourceData,
                                                Map<String, Object> ocsfData) throws JSONException {
        JSONObject ifClause = transform.getJSONObject("if");

        if (evaluateComplexCondition(ifClause.getJSONObject("condition"), sourceData)) {
            applyOperations(ifClause.getJSONArray("operations"), sourceData, ocsfData);
        } else if (transform.has("elseIf")) {
            JSONArray elseIfClauses = transform.getJSONArray("elseIf");
            boolean matched = false;

            for (int i = 0; i < elseIfClauses.length() && !matched; i++) {
                JSONObject elseIfClause = elseIfClauses.getJSONObject(i);
                if (evaluateComplexCondition(elseIfClause.getJSONObject("condition"), sourceData)) {
                    applyOperations(elseIfClause.getJSONArray("operations"), sourceData, ocsfData);
                    matched = true;
                }
            }

            if (!matched && transform.has("else")) {
                applyOperations(transform.getJSONObject("else").getJSONArray("operations"),
                        sourceData, ocsfData);
            }
        } else if (transform.has("else")) {
            applyOperations(transform.getJSONObject("else").getJSONArray("operations"),
                    sourceData, ocsfData);
        }
    }

    private void applyVariableTransformation(JSONObject transform, Map<String, Object> ocsfData) throws JSONException {
        String target = transform.getString("target");
        String variableName = transform.getString("value");
        String type = transform.getString("type");

        String variableValue = getVariableValue(variableName);
        if (variableValue != null) {
            Object convertedValue = convertValue(variableValue, type, transform);
            if (convertedValue != null) {
                setNestedValue(ocsfData, target, convertedValue);
            }
        }
    }

    private String getVariableValue(String variableName) {
        if (variableName.equals("SCHEMA_VERSION")) {
            return schema.optString("ocsf_schema_version", DEFAULT_SCHEMA_VERSION);
        }
        LOG.warn("Unknown variable: {}", variableName);
        return null;
    }

    private boolean evaluateComplexCondition(JSONObject condition, Map<String, Object> sourceData) throws JSONException {
        JSONArray clauses = condition.getJSONArray("clauses");
        String logic = condition.optString("logic", "AND");

        boolean result = !logic.equals("OR");
        for (int i = 0; i < clauses.length(); i++) {
            JSONObject clause = clauses.getJSONObject(i);
            boolean clauseResult = evaluateClause(clause, sourceData);

            if (logic.equals("OR")) {
                result |= clauseResult;
            } else {
                result &= clauseResult;
            }
        }
        return result;
    }

    private boolean evaluateClause(JSONObject clause, Map<String, Object> sourceData) throws JSONException {
        JSONObject lhs = clause.getJSONObject("lhs");
        String operation = clause.getString("operation");
        boolean ignoreCase = clause.optBoolean("ignoreCase", false);

        String fieldName = lhs.getString("value");
        Object fieldValue = getNestedValue(sourceData, fieldName);

        if (fieldValue == null) {
            return false;
        }

        switch (operation) {
            case "EQUALS":
                String pattern = clause.getJSONObject("rhs").getString("value");
                String fieldStr = String.valueOf(fieldValue);
                return ignoreCase ?
                        fieldStr.equalsIgnoreCase(pattern) :
                        fieldStr.matches(pattern);
            case "EXISTS":
                return true;
            case "CONTAINS":
                String searchPattern = clause.getJSONObject("rhs").getString("value");
                return String.valueOf(fieldValue).contains(searchPattern);
            default:
                LOG.warn("Unsupported operation: {}", operation);
                return false;
        }
    }

    private Object convertValue(Object value, String type, JSONObject config) {
        if (value == null) {
            return null;
        }

        try {
            switch (type.toLowerCase()) {
                case "string":
                    return String.valueOf(value);
                case "integer":
                    LOG.debug("Attempting integer conversion for: {}", value);
                    return Integer.valueOf(String.valueOf(value));
                case "long":
                    return Long.valueOf(String.valueOf(value));
                case "boolean":
                    return Boolean.valueOf(String.valueOf(value));
                case "double":
                    return Double.valueOf(String.valueOf(value));
                case "timestamp":
                    if (config.has("format")) {
                        String format = config.getString("format");
                        String formatString = config.optString("format_string");
                        return parseTimestamp(String.valueOf(value), format, formatString);
                    }
                    return value;
                default:
                    LOG.warn("Unsupported type: {}", type);
                    return value;
            }
        } catch (Exception e) {
            LOG.error("Error converting value: {} to type: {}", value, type, e);
            throw new IllegalArgumentException("Failed to convert value: " + value + " to type: " + type);
        }
    }

    private Long parseTimestamp(String value, String format, String formatString) {
        try {
            switch (format) {
                case "unixtimewithformat":
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatString);
                    ZonedDateTime zdt = ZonedDateTime.parse(value, formatter);
                    return zdt.toInstant().toEpochMilli();
                case "unixtime":
                    return Long.parseLong(value);
                case "iso8601":
                    return Instant.parse(value).toEpochMilli();
                default:
                    LOG.warn("Unsupported timestamp format: {}", format);
                    return null;
            }
        } catch (DateTimeParseException e) {
            LOG.error("Error parsing timestamp: {} with format: {}", value, format, e);
            return null;
        }
    }

    private void applyOperations(JSONArray operations,
                                 Map<String, Object> sourceData,
                                 Map<String, Object> ocsfData) throws JSONException {
        for (int i = 0; i < operations.length(); i++) {
            JSONObject operation = operations.getJSONObject(i);
            String operationType = operation.getString("operation");

            switch (operationType) {
                case "mapping":
                    String source = operation.getString("source");
                    String destination = operation.getString("destination");
                    String type = operation.getString("type");

                    Object sourceValue = getNestedValue(sourceData, source);
                    if (sourceValue != null) {
                        Object value = convertValue(sourceValue, type, operation);
                        if (value != null) {
                            setNestedValue(ocsfData, destination, value);
                        }
                    }
                    break;
                case "literal":
                    applyLiteralTransformation(operation, ocsfData);
                    break;
                default:
                    LOG.warn("Unsupported operation type: {}", operationType);
            }
        }
    }
}