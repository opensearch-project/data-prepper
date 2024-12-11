/*
 *
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.neptune.stream.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Setter
@Getter
public class StreamPosition {
    private long commitNum;
    private long opNum;

    @Override
    public String toString() {
        return String.format("StreamPosition [commitNum=%s, opNum=%s]", commitNum, opNum);
    }

    public String asAckString() {
        return String.format("%d-%d", getCommitNum(), getOpNum());
    }

    public static StreamPosition empty() {
        return new StreamPosition(0L, 0L);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StreamPosition that = (StreamPosition) o;
        return commitNum == that.commitNum && opNum == that.opNum;
    }
}
