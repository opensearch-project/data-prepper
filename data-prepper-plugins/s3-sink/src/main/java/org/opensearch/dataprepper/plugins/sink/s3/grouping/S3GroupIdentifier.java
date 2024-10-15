/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.grouping;

import org.opensearch.dataprepper.plugins.sink.s3.ObjectMetadata;
import java.util.Map;
import java.util.Objects;

class S3GroupIdentifier {
    private final Map<String, Object> groupIdentifierHash;
    private final String groupIdentifierFullObjectKey;

    private final Object objectMetadataConfig;
    private final String fullBucketName;

    public S3GroupIdentifier(final Map<String, Object> groupIdentifierHash,
                             final String groupIdentifierFullObjectKey,
                             final Object objectMetadataConfig,
                             final String fullBucketName) {
        this.groupIdentifierHash = groupIdentifierHash;
        this.groupIdentifierFullObjectKey = groupIdentifierFullObjectKey;
        this.objectMetadataConfig = objectMetadataConfig;
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

    public Map<String, String> getMetadata(int eventCount) {
        if (objectMetadataConfig == null) {
            return null;
        }
        ObjectMetadata objectMetadata = new ObjectMetadata(objectMetadataConfig);
	objectMetadata.setEventCount(eventCount);
	return objectMetadata.get();
    }
    public String getFullBucketName() { return fullBucketName; }
}
