package org.opensearch.dataprepper.plugins.source.s3.ownership;

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
class MappedBucketOwnerProvider implements BucketOwnerProvider {
    private final Map<String, String> bucketOwnershipMap;
    private final BucketOwnerProvider fallbackProvider;

    MappedBucketOwnerProvider(Map<String, String> bucketOwnershipMap, BucketOwnerProvider fallbackProvider) {
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
