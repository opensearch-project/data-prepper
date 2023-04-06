/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

class S3SelectOptionsTest {

    @Mock
    private S3SelectCSVOption s3SelectCSVOption;

    @Mock
    private S3SelectJsonOption s3SelectJsonOption;

    @ParameterizedTest
    @CsvSource({"csv","json","parquet"})
    void s3SelectOptionsTest(final String dataSerializationFormat) throws NoSuchFieldException, IllegalAccessException {
        S3SelectOptions s3SelectOptions = new S3SelectOptions();
        final String expression = "select * from s3Object";
        final S3SelectSerializationFormatOption s3SelectSerializationFormatOption = S3SelectSerializationFormatOption.fromOptionValue(dataSerializationFormat);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"expression",expression);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"s3SelectSerializationFormatOption",s3SelectSerializationFormatOption);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"expressionType","SQL");
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"compressionType","none");
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"s3SelectCSVOption",s3SelectCSVOption);
        ReflectivelySetField.setField(S3SelectOptions.class,s3SelectOptions,"s3SelectJsonOption",s3SelectJsonOption);
        assertThat(s3SelectOptions.getExpression(),sameInstance(expression));
        assertThat(s3SelectOptions.getS3SelectSerializationFormatOption(),sameInstance(s3SelectSerializationFormatOption));
        assertThat(s3SelectOptions.getExpressionType(),equalTo("SQL"));
        assertThat(s3SelectOptions.getCompressionType(),equalTo("none"));
        assertThat(s3SelectOptions.getS3SelectCSVOption(),sameInstance(s3SelectCSVOption));
        assertThat(s3SelectOptions.getS3SelectJsonOption(),sameInstance(s3SelectJsonOption));
    }
}
