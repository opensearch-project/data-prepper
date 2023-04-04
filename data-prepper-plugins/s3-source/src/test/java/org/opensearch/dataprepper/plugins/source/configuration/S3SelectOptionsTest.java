/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SelectOptionsTest {

    @ParameterizedTest
    @CsvSource({"csv","json","parquet"})
    void s3SelectOptionsTest(final String dataSerializationFormat) throws NoSuchFieldException, IllegalAccessException {
        S3SelectOptions s3SelectOptions = new S3SelectOptions();
        final String expression = "select * from s3Object";
        final S3SelectSerializationFormatOption s3SelectSerializationFormatOption = S3SelectSerializationFormatOption.fromOptionValue(dataSerializationFormat);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"expression",expression);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"s3SelectSerializationFormatOption",s3SelectSerializationFormatOption);
        assertThat(s3SelectOptions.getExpression(),sameInstance(expression));
        assertThat(s3SelectOptions.getS3SelectSerializationFormatOption(),sameInstance(s3SelectSerializationFormatOption));
    }
}
