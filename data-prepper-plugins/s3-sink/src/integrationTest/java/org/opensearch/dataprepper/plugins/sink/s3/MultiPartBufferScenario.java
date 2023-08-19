/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;

public class MultiPartBufferScenario implements BufferScenario {
    @Override
    public BufferTypeOptions getBufferType() {
        return BufferTypeOptions.MULTI_PART;
    }

    @Override
    public int getMaximumNumberOfEvents() {
        return SizeCombination.LARGE.getTotalSize();
    }
}
