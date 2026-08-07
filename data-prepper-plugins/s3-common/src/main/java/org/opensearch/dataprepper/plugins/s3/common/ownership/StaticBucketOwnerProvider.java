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

import java.util.Objects;
import java.util.Optional;

public class StaticBucketOwnerProvider implements BucketOwnerProvider {
    private final String accountId;

    public StaticBucketOwnerProvider(final String accountId) {
        this.accountId = Objects.requireNonNull(accountId);
    }

    @Override
    public Optional<String> getBucketOwner(final String bucket) {
        return Optional.of(accountId);
    }
}
