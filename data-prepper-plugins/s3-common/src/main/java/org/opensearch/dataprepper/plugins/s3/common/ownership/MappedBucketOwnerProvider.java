/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.s3.common.ownership;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
/**
 * Implements {@link BucketOwnerProvider} using a mapping of bucket
 * names to account Ids for the bucket owners. Uses a delegate
 * {@link BucketOwnerProvider} as a fallback when the bucket is not
 * found in the map.
 */
public class MappedBucketOwnerProvider implements BucketOwnerProvider {
    private final Map<String, String> bucketOwnershipMap;
    private final BucketOwnerProvider fallbackProvider;

    public MappedBucketOwnerProvider(Map<String, String> bucketOwnershipMap, BucketOwnerProvider fallbackProvider) {
        this.bucketOwnershipMap = new HashMap<>(Objects.requireNonNull(bucketOwnershipMap));
        this.fallbackProvider = Objects.requireNonNull(fallbackProvider);
    }

    @Override
    public Optional<String> getBucketOwner(String bucket) {
        String account = bucketOwnershipMap.get(bucket);
        if(account != null) {
            return Optional.of(account);
        }
        return fallbackProvider.getBucketOwner(bucket);
    }
}
