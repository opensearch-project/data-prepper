/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.KeyGenerator;
import org.opensearch.dataprepper.plugins.sink.s3.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.s3.S3BucketSelector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3GroupIdentifierFactory {

    private final KeyGenerator keyGenerator;

    private final List<String> dynamicEventsKeys;

    private final List<String> dynamicExpressions;

    private final ExpressionEvaluator expressionEvaluator;

    private final S3SinkConfig s3SinkConfig;
    private final S3BucketSelector s3BucketSelector;

    private static final String BUCKET_NAME_REPLACEMENT_FOR_NON_EXISTING_KEYS = "";

    public S3GroupIdentifierFactory(final KeyGenerator keyGenerator,
                                    final ExpressionEvaluator expressionEvaluator,
                                    final S3SinkConfig s3SinkConfig,
                                    final S3BucketSelector s3BucketSelector) {
        this.keyGenerator = keyGenerator;
        this.expressionEvaluator = expressionEvaluator;
        this.s3SinkConfig = s3SinkConfig;
        this.s3BucketSelector = s3BucketSelector;

        dynamicExpressions = expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix());
        dynamicExpressions.addAll(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()));
        if (s3BucketSelector == null)
            dynamicExpressions.addAll(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(s3SinkConfig.getBucketName()));

        dynamicEventsKeys = expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getPathPrefix());
        dynamicEventsKeys.addAll(expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getObjectKeyOptions().getNamePattern()));
        if (s3BucketSelector == null)
            dynamicEventsKeys.addAll(expressionEvaluator.extractDynamicKeysFromFormatExpression(s3SinkConfig.getBucketName()));
     }


    public S3GroupIdentifier getS3GroupIdentifierForEvent(final Event event) {

        final String fullObjectKey = keyGenerator.generateKeyForEvent(event);
        final String fullBucketName = s3BucketSelector != null ? s3BucketSelector.getBucketName() :
                event.formatString(s3SinkConfig.getBucketName(), expressionEvaluator, BUCKET_NAME_REPLACEMENT_FOR_NON_EXISTING_KEYS);

        final Map<String, Object> groupIdentificationHash = new HashMap<>();

        for (final String key : dynamicEventsKeys) {
            final Object value = event.get(key, Object.class);
            groupIdentificationHash.put(key, value);
        }

        for (final String expression : dynamicExpressions) {
            final Object value = expressionEvaluator.evaluate(expression, event);
            groupIdentificationHash.put(expression, value);
        }


        return new S3GroupIdentifier(groupIdentificationHash, fullObjectKey, s3SinkConfig.getObjectMetadata(), fullBucketName);
    }
}
