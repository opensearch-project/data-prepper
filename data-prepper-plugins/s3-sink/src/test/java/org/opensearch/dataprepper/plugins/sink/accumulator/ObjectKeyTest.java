/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.codec.JsonCodec;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;

@ExtendWith(MockitoExtension.class)
class ObjectKeyTest {

    @Mock
    private ObjectKey objectKey;
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private PluginModel pluginModel;
    @Mock
    private PluginSetting pluginSetting;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private JsonCodec codec;
    @Mock
    private BucketOptions bucketOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;

    @BeforeEach
    void setUp() throws Exception {
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
    }

    @Test
    void test_buildingPathPrefix() {

        when(objectKeyOptions.getPathPrefix()).thenReturn("events/%{yyyy}/%{MM}/%{dd}/");
        String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        Assertions.assertNotNull(pathPrefix);
        assertThat(pathPrefix, startsWith("events"));
    }

    @Test
    void test_objectFileName() {
        
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig);
        Assertions.assertNotNull(objectFileName);
        assertThat(objectFileName, startsWith("my-elb"));
    }

    @Test
    void test_objectFileName_with_fileExtension() {

        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getNamePattern())
                .thenReturn("events-%{yyyy-MM-dd'T'hh-mm-ss}.pdf");
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig);
        Assertions.assertNotNull(objectFileName);
        Assertions.assertTrue(objectFileName.contains(".pdf"));
    }

    @Test
    void test_objectFileName_default_fileExtension() {

        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions().getNamePattern())
                .thenReturn("events-%{yyyy-MM-dd'T'hh-mm-ss}");
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig);
        Assertions.assertNotNull(objectFileName);
        Assertions.assertTrue(objectFileName.contains(".json"));
    }
}