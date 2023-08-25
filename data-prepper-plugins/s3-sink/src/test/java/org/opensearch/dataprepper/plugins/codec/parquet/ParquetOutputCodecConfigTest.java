package org.opensearch.dataprepper.plugins.codec.parquet;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ParquetOutputCodecConfigTest {

    private ParquetOutputCodecConfig createObjectUnderTest() {
        return new ParquetOutputCodecConfig();
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_true_if_schema_defined() {
        ParquetOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setSchema(UUID.randomUUID().toString());

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(true));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_true_if_schema_null_and_autoSchema_true() {
        ParquetOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setAutoSchema(true);

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(true));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_false_if_schema_null() {
        ParquetOutputCodecConfig objectUnderTest = createObjectUnderTest();

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(false));
    }

    @Test
    void isSchemaOrAutoSchemaDefined_returns_false_if_schema_and_autoSchema() {
        ParquetOutputCodecConfig objectUnderTest = createObjectUnderTest();
        objectUnderTest.setSchema(UUID.randomUUID().toString());
        objectUnderTest.setAutoSchema(true);

        assertThat(objectUnderTest.isSchemaOrAutoSchemaDefined(), equalTo(false));
    }

}