/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.SchemaGenerationContext;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.ExampleValues;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExampleValuesInstanceAttributeOverrideTest {

    @Mock
    private ObjectNode fieldSchema;

    @Mock
    private FieldScope fieldScope;

    @Mock
    private SchemaGenerationContext context;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        fieldSchema = spy(objectMapper.convertValue(Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()), ObjectNode.class));
    }

    private ExampleValuesInstanceAttributeOverride createObjectUnderTest() {
        return new ExampleValuesInstanceAttributeOverride();
    }

    @Test
    void overrideInstanceAttributes_does_not_modify_fieldSchema_if_no_ExampleValues_annotation() {
        createObjectUnderTest().overrideInstanceAttributes(fieldSchema, fieldScope, context);

        verifyNoInteractions(fieldSchema);
    }

    @Nested
    class WithExampleValuesAnnotation {
        @Mock
        private ExampleValues exampleValuesAnnotation;

        @Mock
        private SchemaGeneratorConfig schemaGeneratorConfig;

        @BeforeEach
        void setUp() {
            when(fieldScope.getAnnotationConsideringFieldAndGetterIfSupported(ExampleValues.class))
                    .thenReturn(exampleValuesAnnotation);
        }

        @Test
        void overrideInstanceAttributes_does_not_modify_fieldSchema_if_no_ExampleValues_annotation_is_empty() {
            when(exampleValuesAnnotation.value()).thenReturn(new ExampleValues.Example[]{});

            createObjectUnderTest().overrideInstanceAttributes(fieldSchema, fieldScope, context);

            verifyNoInteractions(fieldSchema);
        }

        @Test
        void overrideInstanceAttributes_adds_examples_when_one_ExampleValue() {
            when(context.getGeneratorConfig()).thenReturn(schemaGeneratorConfig);
            when(schemaGeneratorConfig.getObjectMapper()).thenReturn(objectMapper);

            final ExampleValues.Example example = mock(ExampleValues.Example.class);
            final String value = UUID.randomUUID().toString();
            final String description = UUID.randomUUID().toString();
            when(example.value()).thenReturn(value);
            when(example.description()).thenReturn(description);
            final ExampleValues.Example[] examples = {example};
            when(exampleValuesAnnotation.value()).thenReturn(examples);

            createObjectUnderTest().overrideInstanceAttributes(fieldSchema, fieldScope, context);

            final JsonNode examplesNode = fieldSchema.get("examples");
            assertThat(examplesNode, notNullValue());

            assertThat(examplesNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
            assertThat(examplesNode.size(), equalTo(1));
            final JsonNode firstExampleNode = examplesNode.get(0);
            assertThat(firstExampleNode, notNullValue());
            assertThat(firstExampleNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
            assertThat(firstExampleNode.get("example"), notNullValue());
            assertThat(firstExampleNode.get("example").getNodeType(), equalTo(JsonNodeType.STRING));
            assertThat(firstExampleNode.get("example").textValue(), equalTo(value));
            assertThat(firstExampleNode.get("description"), notNullValue());
            assertThat(firstExampleNode.get("description").getNodeType(), equalTo(JsonNodeType.STRING));
            assertThat(firstExampleNode.get("description").textValue(), equalTo(description));
        }

        @Test
        void overrideInstanceAttributes_adds_examples_when_one_ExampleValue_with_no_description() {
            when(context.getGeneratorConfig()).thenReturn(schemaGeneratorConfig);
            when(schemaGeneratorConfig.getObjectMapper()).thenReturn(objectMapper);

            final ExampleValues.Example example = mock(ExampleValues.Example.class);
            final String value = UUID.randomUUID().toString();
            final String description = UUID.randomUUID().toString();
            when(example.value()).thenReturn(value);
            final ExampleValues.Example[] examples = {example};
            when(exampleValuesAnnotation.value()).thenReturn(examples);

            createObjectUnderTest().overrideInstanceAttributes(fieldSchema, fieldScope, context);

            final JsonNode examplesNode = fieldSchema.get("examples");
            assertThat(examplesNode, notNullValue());

            assertThat(examplesNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
            assertThat(examplesNode.size(), equalTo(1));
            final JsonNode firstExampleNode = examplesNode.get(0);
            assertThat(firstExampleNode, notNullValue());
            assertThat(firstExampleNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
            assertThat(firstExampleNode.get("example"), notNullValue());
            assertThat(firstExampleNode.get("example").getNodeType(), equalTo(JsonNodeType.STRING));
            assertThat(firstExampleNode.get("example").textValue(), equalTo(value));
            assertThat(firstExampleNode.has("description"), equalTo(false));
        }

        @ParameterizedTest
        @ValueSource(ints = {2, 3, 5})
        void overrideInstanceAttributes_adds_examples_when_multiple_ExampleValue(final int numberOfExamples) {
            when(context.getGeneratorConfig()).thenReturn(schemaGeneratorConfig);
            when(schemaGeneratorConfig.getObjectMapper()).thenReturn(objectMapper);

            final ExampleValues.Example[] examples = new ExampleValues.Example[numberOfExamples];
            for (int i = 0; i < numberOfExamples; i++) {
                final ExampleValues.Example example = mock(ExampleValues.Example.class);
                final String value = UUID.randomUUID().toString();
                final String description = UUID.randomUUID().toString();
                when(example.value()).thenReturn(value);
                when(example.description()).thenReturn(description);

                examples[i] = example;
            }
            when(exampleValuesAnnotation.value()).thenReturn(examples);

            createObjectUnderTest().overrideInstanceAttributes(fieldSchema, fieldScope, context);

            final JsonNode examplesNode = fieldSchema.get("examples");
            assertThat(examplesNode, notNullValue());

            assertThat(examplesNode.getNodeType(), equalTo(JsonNodeType.ARRAY));
            assertThat(examplesNode.size(), equalTo(numberOfExamples));

            for (int i = 0; i < numberOfExamples; i++) {
                final JsonNode exampleNode = examplesNode.get(0);
                assertThat(exampleNode, notNullValue());
                assertThat(exampleNode.getNodeType(), equalTo(JsonNodeType.OBJECT));
                assertThat(exampleNode.get("example"), notNullValue());
                assertThat(exampleNode.get("example").getNodeType(), equalTo(JsonNodeType.STRING));
                assertThat(exampleNode.get("example").textValue(), notNullValue());
                assertThat(exampleNode.get("description"), notNullValue());
                assertThat(exampleNode.get("description").getNodeType(), equalTo(JsonNodeType.STRING));
                assertThat(exampleNode.get("description").textValue(), notNullValue());
            }
        }
    }
}