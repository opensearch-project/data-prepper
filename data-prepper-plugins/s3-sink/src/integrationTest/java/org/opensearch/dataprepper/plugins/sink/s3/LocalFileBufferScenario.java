/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;

import static org.opensearch.dataprepper.plugins.sink.s3.S3SinkIT.LARGE_OBJECT_SIZE;

public class LocalFileBufferScenario implements BufferScenario {
    @Override
    public BufferTypeOptions getBufferType() {
        return BufferTypeOptions.LOCALFILE;
    }

    @Override
    public int getMaximumNumberOfEvents() {
        return LARGE_OBJECT_SIZE;
    }
}
