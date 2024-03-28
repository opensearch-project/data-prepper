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
    private final ExtensionProvider extensionProvider;

    private final ExpressionEvaluator expressionEvaluator;

    public KeyGenerator(final S3SinkConfig s3SinkConfig,
                        final ExtensionProvider extensionProvider,
                        final ExpressionEvaluator expressionEvaluator) {
        this.s3SinkConfig = s3SinkConfig;
        this.extensionProvider = extensionProvider;
        this.expressionEvaluator = expressionEvaluator;
    }

    /**
     * Generate the s3 object path prefix and object file name.
     *
     * @return object key path.
     */
    public String generateKeyForEvent(final Event event, final boolean includeDateAndCodec) {
        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig, event, expressionEvaluator);
        final String namePattern = includeDateAndCodec ? ObjectKey.objectFileName(s3SinkConfig, extensionProvider.getExtension(), event, expressionEvaluator) :
                ObjectKey.objectFileNameWithoutDateTimeAndCodecInjection(s3SinkConfig, event, expressionEvaluator);
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }
}
