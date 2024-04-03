/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.KeyGenerator;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3GroupIdentifierFactory {

    private final KeyGenerator keyGenerator;

    private final List<String> dynamicEventsKeys;

    private final List<String> dynamicExpressions;

    private final ExpressionEvaluator expressionEvaluator;

    private final S3SinkConfig s3SinkConfig;

    public S3GroupIdentifierFactory(final KeyGenerator keyGenerator,
                                    final ExpressionEvaluator expressionEvaluator,
                                    final S3SinkConfig s3SinkConfig) {
        this.keyGenerator = keyGenerator;
        this.expressionEvaluator = expressionEvaluator;
        this.s3SinkConfig = s3SinkConfig;

        dynamicExpressions = expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix());
        dynamicExpressions.addAll(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()));

        dynamicEventsKeys = expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix());
        dynamicEventsKeys.addAll(expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()));
     }


    public S3GroupIdentifier getS3GroupIdentifierForEvent(final Event event) {

        final String fullObjectKey = keyGenerator.generateKeyForEvent(event);
        final Map<String, Object> groupIdentificationHash = new HashMap<>();

        for (final String key : dynamicEventsKeys) {
            final Object value = event.get(key, Object.class);
            groupIdentificationHash.put(key, value);
        }

        for (final String expression : dynamicExpressions) {
            final Object value = expressionEvaluator.evaluate(expression, event);
            groupIdentificationHash.put(expression, value);
        }

        return new S3GroupIdentifier(groupIdentificationHash, fullObjectKey);
    }
}
