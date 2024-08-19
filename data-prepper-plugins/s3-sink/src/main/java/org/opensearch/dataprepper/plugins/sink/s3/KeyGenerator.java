/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.ObjectKey;


public class KeyGenerator {
    private final S3SinkConfig s3SinkConfig;
    private final S3BucketSelector s3BucketSelector;
    private final ExtensionProvider extensionProvider;

    private final ExpressionEvaluator expressionEvaluator;

    public KeyGenerator(final S3SinkConfig s3SinkConfig,
                        final S3BucketSelector s3BucketSelector,
                        final ExtensionProvider extensionProvider,
                        final ExpressionEvaluator expressionEvaluator) {
        this.s3SinkConfig = s3SinkConfig;
        this.s3BucketSelector = s3BucketSelector;
        this.extensionProvider = extensionProvider;
        this.expressionEvaluator = expressionEvaluator;
    }

    /**
     * Generate the s3 object path prefix and object file name.
     *
     * @return object key path.
     */
    public String generateKeyForEvent(final Event event) {
        final String pathPrefix = s3BucketSelector != null ? s3BucketSelector.getPathPrefix() : ObjectKey.buildingPathPrefix(s3SinkConfig, event, expressionEvaluator);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig, extensionProvider.getExtension(), event, expressionEvaluator);
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }
}
