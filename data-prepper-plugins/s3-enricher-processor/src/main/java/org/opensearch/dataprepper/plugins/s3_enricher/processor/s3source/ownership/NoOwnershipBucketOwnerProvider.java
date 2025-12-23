/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.s3_enricher.processor.s3source.ownership;

import java.util.Optional;

/**
 * An implementation of {@link BucketOwnerProvider} which does not provide
 * a bucket owner, effectively skipping owner validation.
 */
class NoOwnershipBucketOwnerProvider implements BucketOwnerProvider {
    @Override
    public Optional<String> getBucketOwner(final String bucket) {
        return Optional.empty();
    }
}
