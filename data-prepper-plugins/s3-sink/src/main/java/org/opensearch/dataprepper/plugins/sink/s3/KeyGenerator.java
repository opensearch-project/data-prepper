/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.accumulator.ObjectKey;

public class KeyGenerator {
    private final S3SinkConfig s3SinkConfig;
    private final ExtensionProvider extensionProvider;

    public KeyGenerator(S3SinkConfig s3SinkConfig, ExtensionProvider extensionProvider) {
        this.s3SinkConfig = s3SinkConfig;
        this.extensionProvider = extensionProvider;
    }

    /**
     * Generate the s3 object path prefix and object file name.
     *
     * @return object key path.
     */
    String generateKey() {
        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig, extensionProvider.getExtension());
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }
}
