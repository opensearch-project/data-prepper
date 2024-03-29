/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.accumulator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ObjectKeyTest {

    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private ObjectKeyOptions objectKeyOptions;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Event event;

    @BeforeEach
    void setUp() throws Exception {
        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);
    }

    @Test
    void test_buildingPathPrefix() {
        final String pathPrefix = "events/%{yyyy}/%{MM}/%{dd}/";

        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix);
        when(event.formatString(pathPrefix, expressionEvaluator)).thenReturn(pathPrefix);
        String pathPrefixResult = ObjectKey.buildingPathPrefix(s3SinkConfig, event, expressionEvaluator);
        Assertions.assertNotNull(pathPrefixResult);
        assertThat(pathPrefixResult, startsWith("events"));
    }

    @Test
    void test_objectFileName() {
        final String namePattern = "my-elb-%{yyyy-MM-dd'T'hh-mm-ss}";

        when(objectKeyOptions.getNamePattern()).thenReturn(namePattern);
        when(event.formatString(namePattern, expressionEvaluator)).thenReturn(namePattern);
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig, null, event, expressionEvaluator);
        Assertions.assertNotNull(objectFileName);
        assertThat(objectFileName, startsWith("my-elb"));
    }

    @Test
    void test_objectFileName_with_fileExtension() {
        final String namePattern = "events-%{yyyy-MM-dd'T'hh-mm-ss}.pdf";

        when(s3SinkConfig.getObjectKeyOptions().getNamePattern())
                .thenReturn(namePattern);
        when(event.formatString(namePattern, expressionEvaluator)).thenReturn(namePattern);
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig, null, event, expressionEvaluator);
        Assertions.assertNotNull(objectFileName);
        Assertions.assertTrue(objectFileName.contains(".pdf"));
    }

    @Test
    void test_objectFileName_default_fileExtension() {
        final String namePattern = "events-%{yyyy-MM-dd'T'hh-mm-ss}";

        when(s3SinkConfig.getObjectKeyOptions().getNamePattern())
                .thenReturn(namePattern);
        when(event.formatString(namePattern, expressionEvaluator)).thenReturn(namePattern);
        String objectFileName = ObjectKey.objectFileName(s3SinkConfig, null, event, expressionEvaluator);
        Assertions.assertNotNull(objectFileName);
        Assertions.assertTrue(objectFileName.contains(".json"));
    }
}