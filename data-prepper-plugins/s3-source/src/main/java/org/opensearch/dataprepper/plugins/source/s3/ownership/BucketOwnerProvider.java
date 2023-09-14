/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3.ownership;

import java.util.Optional;

/**
 * Gets the expected owner of an S3 bucket.
 */
@FunctionalInterface
public interface BucketOwnerProvider {
    /**
     * Gets the accountId of the owner bucket. Returns an empty optional
     * if no account owner is known.
     * @param bucket the name of the bucket
     * @return The accountId or empty
     */
    Optional<String> getBucketOwner(final String bucket);
}
