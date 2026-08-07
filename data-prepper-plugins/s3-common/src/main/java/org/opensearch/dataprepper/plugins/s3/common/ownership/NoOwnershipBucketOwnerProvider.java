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

import java.util.Optional;

/**
 * An implementation of {@link BucketOwnerProvider} which does not provide
 * a bucket owner, effectively skipping owner validation.
 */
public class NoOwnershipBucketOwnerProvider implements BucketOwnerProvider{
    @Override
    public Optional<String> getBucketOwner(final String bucket) {
        return Optional.empty();
    }
}
