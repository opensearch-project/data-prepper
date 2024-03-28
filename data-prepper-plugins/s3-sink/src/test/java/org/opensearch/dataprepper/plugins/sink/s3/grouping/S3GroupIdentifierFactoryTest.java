/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.KeyGenerator;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ObjectKeyOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class S3GroupIdentifierFactoryTest {

    private KeyGenerator keyGenerator;

    private S3SinkConfig s3SinkConfig;

    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setup() {
        keyGenerator = mock(KeyGenerator.class);
        expressionEvaluator = mock(ExpressionEvaluator.class);
        s3SinkConfig = mock(S3SinkConfig.class);

        final String pathPrefix = UUID.randomUUID().toString();
        final String objectName = UUID.randomUUID().toString();
        final ObjectKeyOptions objectKeyOptions = mock(ObjectKeyOptions.class);
        when(objectKeyOptions.getNamePattern()).thenReturn(objectName);
        when(objectKeyOptions.getPathPrefix()).thenReturn(pathPrefix);

        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);
    }

    private S3GroupIdentifierFactory createObjectUnderTest() {
        return new S3GroupIdentifierFactory(keyGenerator, expressionEvaluator, s3SinkConfig);
    }

    @Test
    void getS3GroupIdentifierForEvent_returns_expected_s3GroupIdentifier() {
        final String dynamicKeyPathPrefix = UUID.randomUUID().toString();
        final String dynamicValuePathPrefix = UUID.randomUUID().toString();
        final String dynamicExpressionPathPrefix = UUID.randomUUID().toString();
        final String dynamicExpressionResultPathPrefix = UUID.randomUUID().toString();
        final String dynamicKeyObjectName = UUID.randomUUID().toString();
        final String dynamicValueObjectName = UUID.randomUUID().toString();
        final String dynamicExpressionObjectName = UUID.randomUUID().toString();
        final String dynamicExpressionResultObjectName = UUID.randomUUID().toString();

        final Map<String, Object> expectedIdentificationHash = Map.of(
                dynamicKeyPathPrefix, dynamicValuePathPrefix,
                dynamicExpressionPathPrefix, dynamicExpressionResultPathPrefix,
                dynamicKeyObjectName, dynamicValueObjectName,
                dynamicExpressionObjectName, dynamicExpressionResultObjectName
        );
        final String expectedFullObjectKey = UUID.randomUUID().toString();
        final Event event = mock(Event.class);

        final List<String> expectedDynamicKeysPathPrefix = new ArrayList<>();
        expectedDynamicKeysPathPrefix.add(dynamicKeyPathPrefix);

        final List<String> expectedDynamicExpressionsPathPrefix = new ArrayList<>();
        expectedDynamicExpressionsPathPrefix.add(dynamicExpressionPathPrefix);

        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix()))
                .thenReturn(expectedDynamicExpressionsPathPrefix);
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix()))
                .thenReturn(expectedDynamicKeysPathPrefix);
        when(event.get(dynamicKeyPathPrefix, Object.class)).thenReturn(dynamicValuePathPrefix);
        when(expressionEvaluator.evaluate(dynamicExpressionPathPrefix, event))
                .thenReturn(dynamicExpressionResultPathPrefix);

        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()))
                .thenReturn(List.of(dynamicExpressionObjectName));
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()))
                .thenReturn(List.of(dynamicKeyObjectName));
        when(event.get(dynamicKeyObjectName, Object.class)).thenReturn(dynamicValueObjectName);
        when(expressionEvaluator.evaluate(dynamicExpressionObjectName, event))
                .thenReturn(dynamicExpressionResultObjectName);

        when(keyGenerator.generateKeyForEvent(event)).thenReturn(expectedFullObjectKey);

        final S3GroupIdentifierFactory objectUnderTest = createObjectUnderTest();

        final S3GroupIdentifier result = objectUnderTest.getS3GroupIdentifierForEvent(event);

        assertThat(result, notNullValue());
        assertThat(result.getGroupIdentifierFullObjectKey(), equalTo(expectedFullObjectKey));
        assertThat(result.getGroupIdentifierHash(), equalTo(expectedIdentificationHash));
    }
}
