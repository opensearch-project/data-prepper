/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.plugins.sink.s3.PredefinedObjectMetadata;
import java.util.Map;
import java.util.Objects;

class S3GroupIdentifier {
    private final Map<String, Object> groupIdentifierHash;
    private final String groupIdentifierFullObjectKey;

    private final PredefinedObjectMetadata predefinedObjectMetadata;
    private final String fullBucketName;

    public S3GroupIdentifier(final Map<String, Object> groupIdentifierHash,
                             final String groupIdentifierFullObjectKey,
                             final PredefinedObjectMetadata predefineObjectMetadata,
                             final String fullBucketName) {
        this.groupIdentifierHash = groupIdentifierHash;
        this.groupIdentifierFullObjectKey = groupIdentifierFullObjectKey;
        this.predefinedObjectMetadata = predefineObjectMetadata;
        this.fullBucketName = fullBucketName;
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

    public Map<String, String> getMetadata(int eventCount) { return predefinedObjectMetadata != null ? Map.of(predefinedObjectMetadata.getNumberOfObjects(), Integer.toString(eventCount)) : null; }
    public String getFullBucketName() { return fullBucketName; }
}
