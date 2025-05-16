/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

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
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.MAPPING_OPTIONS;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.MAPPING_SELECTOR;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.DIRECT_MAPPINGS;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.TRANSFORMATION;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.TRANSFORMATIONS;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.DESTINATION;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.CONDITION;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.CASES;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.DEFAULT;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.VALUE;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.SOURCE;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.TYPE;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.RHS;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.LHS;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.OPERATION;
import static org.opensearch.dataprepper.plugins.processor.ocsf.utils.Constants.OPERATIONS;


/**
 * Maps source data to OCSF format according to a provided schema.
 * Supports direct field mappings and transformations (literal, enum, conditional, variable).
 */
public class OcsfSchemaMapper {
    private static final Logger LOG = LoggerFactory.getLogger(OcsfSchemaMapper.class);
    private static final String DEFAULT_SCHEMA_VERSION = "1.0.0-rc.2";
    private final JSONObject schema;

    /**
     * Creates mapper using the provided schema file.
     * @param schemaPath Path to the JSON schema file
     * @throws InvalidPluginConfigurationException if schema is invalid
     */
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
        if (!schema.has(MAPPING_OPTIONS)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain '%s' section", MAPPING_OPTIONS));
        }
        if (!schema.has(MAPPING_SELECTOR)) {
            throw new InvalidPluginConfigurationException(
                    String.format("Schema must contain '%s' section", MAPPING_SELECTOR));
        }
    }

    /**
     * Maps source data to OCSF format by applying mappings and transformations.
     * @param sourceData The source data to transform
     * @return Map containing OCSF-formatted data
     */
    public Map<String, Object> mapToOcsf(Map<String, Object> sourceData) {
        try {
            String mappingType = selectMapping(sourceData);
            LOG.debug("Selected mapping type: {}", mappingType);

            JSONObject mappingOptions = schema.getJSONObject("mapping_options")
                    .getJSONObject(mappingType);

            Map<String, Object> ocsfData = new HashMap<>();

            applyDirectMappings(sourceData, mappingOptions.getJSONArray(DIRECT_MAPPINGS), ocsfData);
            applyTransformations(sourceData, mappingOptions.getJSONArray(TRANSFORMATIONS), ocsfData);

            return ocsfData;
        } catch (Exception e) {
            LOG.error("Error mapping to OCSF: {}", e.getMessage());
            throw new RuntimeException("Failed to map data to OCSF", e);
        }
    }

    private String selectMapping(Map<String, Object> sourceData) throws JSONException {
        JSONObject selector = schema.getJSONObject(MAPPING_SELECTOR);
        JSONArray cases = selector.getJSONArray(CASES);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject caseObj = cases.getJSONObject(i);
            if (evaluateComplexCondition(caseObj.getJSONObject(CONDITION), sourceData)) {
                return caseObj.getString("selection");
            }
        }

        return selector.getString("default_selection");
    }

    /**
     * Applies direct field mappings from source to OCSF format.
     * Handles both simple field mappings and array-based mappings.
     */
    private void applyDirectMappings(Map<String, Object> sourceData,
                                     JSONArray directMappings,
                                     Map<String, Object> ocsfData) throws JSONException {
        for (int i = 0; i < directMappings.length(); i++) {
            JSONObject mapping = directMappings.getJSONObject(i);
            String source = mapping.getString(SOURCE);
            String destination = mapping.getString(DESTINATION);
            String type = mapping.getString(TYPE);

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

    /**
     * Applies transformations to the data. Supported transformations:
     * - literal: Sets a fixed value
     * - enum: Applies transformations by mapping source values to predefined OCSF values
     * - conditional: Applies transformations based on conditions
     * - variable: Sets values from predefined variables
     */
    private void applyTransformations(Map<String, Object> sourceData,
                                      JSONArray transformations,
                                      Map<String, Object> ocsfData) throws JSONException {
        for (int i = 0; i < transformations.length(); i++) {
            JSONObject transform = transformations.getJSONObject(i);
            String transformationType = transform.getString(TRANSFORMATION);

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
        Object value = transform.get(VALUE);
        String type = transform.getString(TYPE);

        Object convertedValue = convertValue(value, type, transform);
        if (convertedValue != null) {
            setNestedValue(ocsfData, target, convertedValue);
        }
    }

    private void applyEnumTransformation(JSONObject transform, Map<String, Object> sourceData, Map<String, Object> ocsfData) throws JSONException {
        String source = transform.getString(SOURCE);
        String target = transform.getString("target");
        String type = transform.getString(TYPE);
        boolean ignoreCase = transform.optBoolean("ignoreCase", false);

        Object sourceValue = getNestedValue(sourceData, source);
        if (sourceValue != null) {
            JSONArray cases = transform.getJSONArray(CASES);

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

            if (transform.has(DEFAULT)) {
                Object defaultValue = convertValue(transform.get(DEFAULT), type, transform);
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

        if (evaluateComplexCondition(ifClause.getJSONObject(CONDITION), sourceData)) {
            applyOperations(ifClause.getJSONArray(OPERATIONS), sourceData, ocsfData);
        } else if (transform.has("elseIf")) {
            JSONArray elseIfClauses = transform.getJSONArray("elseIf");
            boolean matched = false;

            for (int i = 0; i < elseIfClauses.length() && !matched; i++) {
                JSONObject elseIfClause = elseIfClauses.getJSONObject(i);
                if (evaluateComplexCondition(elseIfClause.getJSONObject(CONDITION), sourceData)) {
                    applyOperations(elseIfClause.getJSONArray(OPERATIONS), sourceData, ocsfData);
                    matched = true;
                }
            }

            if (!matched && transform.has("else")) {
                applyOperations(transform.getJSONObject("else").getJSONArray(OPERATIONS),
                        sourceData, ocsfData);
            }
        } else if (transform.has("else")) {
            applyOperations(transform.getJSONObject("else").getJSONArray(OPERATIONS),
                    sourceData, ocsfData);
        }
    }

    private void applyVariableTransformation(JSONObject transform, Map<String, Object> ocsfData) throws JSONException {
        String target = transform.getString("target");
        String variableName = transform.getString(VALUE);
        String type = transform.getString(TYPE);

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
        JSONObject lhs = clause.getJSONObject(LHS);
        String operation = clause.getString(OPERATION);
        boolean ignoreCase = clause.optBoolean("ignoreCase", false);

        String fieldName = lhs.getString(VALUE);
        Object fieldValue = getNestedValue(sourceData, fieldName);

        if (fieldValue == null) {
            return false;
        }

        switch (operation) {
            case "EQUALS":
                String pattern = clause.getJSONObject(RHS).getString(VALUE);
                String fieldStr = String.valueOf(fieldValue);
                return ignoreCase ?
                        fieldStr.equalsIgnoreCase(pattern) :
                        fieldStr.matches(pattern);
            case "EXISTS":
                return true;
            case "CONTAINS":
                String searchPattern = clause.getJSONObject(RHS).getString(VALUE);
                return String.valueOf(fieldValue).contains(searchPattern);
            default:
                LOG.warn("Unsupported operation: {}", operation);
                return false;
        }
    }

    /**
     * Converts values to specified types (string, integer, long, boolean, double, timestamp).
     * @throws IllegalArgumentException if conversion fails
     */
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
            String operationType = operation.getString(OPERATION);

            switch (operationType) {
                case "mapping":
                    String source = operation.getString(SOURCE);
                    String destination = operation.getString(DESTINATION);
                    String type = operation.getString(TYPE);

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