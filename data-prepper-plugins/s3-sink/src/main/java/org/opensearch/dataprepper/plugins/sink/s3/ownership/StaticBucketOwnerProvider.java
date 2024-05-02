/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3.ownership;

import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link BucketOwnerProvider} which provides the
 * same owner for all buckets.
 */
class StaticBucketOwnerProvider implements BucketOwnerProvider {
    private final String accountId;

    public StaticBucketOwnerProvider(final String accountId) {
        this.accountId = Objects.requireNonNull(accountId);
    }

    @Override
    public Optional<String> getBucketOwner(final String bucket) {
        return Optional.of(accountId);
    }
}
