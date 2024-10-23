/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.InstanceAttributeOverrideV2;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;
import org.opensearch.dataprepper.model.annotations.ExampleValues;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ExampleValuesInstanceAttributeOverride implements InstanceAttributeOverrideV2<FieldScope> {
    @Override
    public void overrideInstanceAttributes(final ObjectNode fieldSchema, final FieldScope fieldScope, final SchemaGenerationContext context) {
        final ExampleValues exampleValuesAnnotation = fieldScope.getAnnotationConsideringFieldAndGetterIfSupported(ExampleValues.class);
        if(exampleValuesAnnotation != null && exampleValuesAnnotation.value().length > 0) {
            final ObjectMapper objectMapper = context.getGeneratorConfig().getObjectMapper();

            addExampleSchema(fieldSchema, objectMapper, exampleValuesAnnotation);
        }
    }

    private void addExampleSchema(final ObjectNode fieldSchema, final ObjectMapper objectMapper, final ExampleValues exampleValuesAnnotation) {
        final List<Map<String, String>> exampleValues = Arrays.stream(exampleValuesAnnotation.value())
                .map(ExampleValuesInstanceAttributeOverride::createExampleMap).collect(Collectors.toList());
        final ArrayNode exampleNode = objectMapper.convertValue(exampleValues, ArrayNode.class);

        fieldSchema.putArray("examples")
                .addAll(exampleNode);
    }

    private static Map<String, String> createExampleMap(final ExampleValues.Example example) {
        final HashMap<String, String> exampleMap = new HashMap<>();
        exampleMap.put("example", example.value());
        if(example.description() != null && !example.description().isEmpty()) {
            exampleMap.put("description", example.description());
        }
        return exampleMap;
    }
}
