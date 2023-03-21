/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SelectOptionsTest {

    @ParameterizedTest
    @CsvSource({"csv","json","parquet"})
    void s3SelectOptionsTest(final String dataSerializationFormat) throws NoSuchFieldException, IllegalAccessException {
        S3SelectOptions s3SelectOptions = new S3SelectOptions();
        final String queryStatement = "select * from s3Object";
        final S3SelectSerializationFormatOption s3SelectSerializationFormatOption = S3SelectSerializationFormatOption.fromOptionValue(dataSerializationFormat);
        reflectivelySetField(s3SelectOptions,"queryStatement",queryStatement);
        reflectivelySetField(s3SelectOptions,"s3SelectSerializationFormatOption",s3SelectSerializationFormatOption);
        assertThat(s3SelectOptions.getQueryStatement(),sameInstance(queryStatement));
        assertThat(s3SelectOptions.getS3SelectSerializationFormatOption(),sameInstance(s3SelectSerializationFormatOption));
    }
    private void reflectivelySetField(final S3SelectOptions s3SelectOptions, final String fieldName, final Object value)
            throws NoSuchFieldException, IllegalAccessException {
        final Field field = S3SelectOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(s3SelectOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}
