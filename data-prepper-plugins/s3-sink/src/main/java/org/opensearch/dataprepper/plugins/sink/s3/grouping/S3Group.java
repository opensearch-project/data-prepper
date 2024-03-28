/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;

public class S3Group {

    private final Buffer buffer;

    private final S3GroupIdentifier s3GroupIdentifier;

    public S3Group(final S3GroupIdentifier s3GroupIdentifier,
                   final Buffer buffer) {
        this.buffer = buffer;
        this.s3GroupIdentifier = s3GroupIdentifier;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    S3GroupIdentifier getS3GroupIdentifier() { return s3GroupIdentifier; }
}
