/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.ownership;

import java.util.Optional;

/**
 * Gets the expected owner of an S3 bucket.
 */
@FunctionalInterface
public interface BucketOwnerProvider {
    Optional<String> getBucketOwner(final String bucket);
}
