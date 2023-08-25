package org.opensearch.dataprepper.plugins.codec.avro;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class AvroOutputCodecConfigTest {

    private AvroOutputCodecConfig createObjectUnderTest() {
        return new AvroOutputCodecConfig();
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_true_if_schema_defined() {
        AvroOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setSchema(UUID.randomUUID().toString());

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(true));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_true_if_schema_null_and_autoSchema_true() {
        AvroOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setAutoSchema(true);

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(true));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_false_if_schema_null() {
        AvroOutputCodecConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(false));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_false_if_schema_and_autoSchema() {
        AvroOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setSchema(UUID.randomUUID().toString());
        objectUnderTest.setAutoSchema(true);

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(false));
    }
}