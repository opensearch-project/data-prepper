/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.plugins.sink.s3.accumulator.Buffer;

public class S3Group {

    private final Buffer buffer;

    public S3Group(final Buffer buffer) {
        this.buffer = buffer;
    }

    public Buffer getBuffer() {
        return buffer;
    }
}
