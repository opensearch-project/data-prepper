/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import java.util.Map;
import java.util.Objects;

class S3GroupIdentifier {
    private final Map<String, Object> groupIdentifierHash;
    private final String groupIdentifierFullObjectKey;

    public S3GroupIdentifier(final Map<String, Object> groupIdentifierHash,
                             final String groupIdentifierFullObjectKey) {
        this.groupIdentifierHash = groupIdentifierHash;
        this.groupIdentifierFullObjectKey = groupIdentifierFullObjectKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3GroupIdentifier that = (S3GroupIdentifier) o;
        return Objects.equals(groupIdentifierHash, that.groupIdentifierHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupIdentifierHash);
    }

    public String getGroupIdentifierFullObjectKey() { return groupIdentifierFullObjectKey; }

    public Map<String, Object> getGroupIdentifierHash() { return groupIdentifierHash; }
}
