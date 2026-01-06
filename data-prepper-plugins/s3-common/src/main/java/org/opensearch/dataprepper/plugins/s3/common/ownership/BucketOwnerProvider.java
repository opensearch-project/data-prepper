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
