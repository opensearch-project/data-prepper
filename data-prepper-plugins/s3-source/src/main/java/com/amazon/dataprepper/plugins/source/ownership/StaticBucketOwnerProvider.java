/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.ownership;

import java.util.Objects;
import java.util.Optional;

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
