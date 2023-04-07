/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.configuration;

import org.junit.Test;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class S3ScanBucketOptionTest {
    @Test
    public void s3ScanBucketOptionTest() throws NoSuchFieldException, IllegalAccessException {
        final S3ScanBucketOption s3ScanBucketOption = new S3ScanBucketOption();
        final String bucketName = "my-bucket-1";
        final PluginModel plugInModel = mock(PluginModel.class);
        final List<String> stringList = Arrays.asList("file1.csv", "file2.csv");
        final S3SelectOptions s3SelectOptions = mock(S3SelectOptions.class);
        ReflectivelySetField.setField(S3ScanBucketOption.class,s3ScanBucketOption,"name",bucketName);
        ReflectivelySetField.setField(S3ScanBucketOption.class,s3ScanBucketOption,"codec",plugInModel);
        ReflectivelySetField.setField(S3ScanBucketOption.class,s3ScanBucketOption,"s3SelectOptions",s3SelectOptions);
        ReflectivelySetField.setField(S3ScanBucketOption.class,s3ScanBucketOption,"keyPath", stringList);
        ReflectivelySetField.setField(S3ScanBucketOption.class,s3ScanBucketOption,"compression", CompressionOption.NONE);

        assertThat(s3ScanBucketOption.getName(), notNullValue());
        assertThat(s3ScanBucketOption.getCodec(), notNullValue());
        assertThat(s3ScanBucketOption.getS3SelectOptions(), notNullValue());
        assertThat(s3ScanBucketOption.getKeyPath(), notNullValue());
        assertThat(s3ScanBucketOption.getName(),equalTo(bucketName));
        assertThat(s3ScanBucketOption.getCodec(),sameInstance(plugInModel));
        assertThat(s3ScanBucketOption.getS3SelectOptions(),sameInstance(s3SelectOptions));
        assertThat(s3ScanBucketOption.getKeyPath(),sameInstance(stringList));
        assertThat(s3ScanBucketOption.getCompression(),sameInstance(CompressionOption.NONE));
    }
}
