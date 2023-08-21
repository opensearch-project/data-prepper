/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.ObjectKey;

public class KeyGenerator {
    private final S3SinkConfig s3SinkConfig;
    private final OutputCodec outputCodec;

    public KeyGenerator(S3SinkConfig s3SinkConfig, OutputCodec outputCodec) {
        this.s3SinkConfig = s3SinkConfig;
        this.outputCodec = outputCodec;
    }

    /**
     * Generate the s3 object path prefix and object file name.
     *
     * @return object key path.
     */
    String generateKey() {
        final String pathPrefix = ObjectKey.buildingPathPrefix(s3SinkConfig);
        final String namePattern = ObjectKey.objectFileName(s3SinkConfig, outputCodec.getExtension());
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }
}
