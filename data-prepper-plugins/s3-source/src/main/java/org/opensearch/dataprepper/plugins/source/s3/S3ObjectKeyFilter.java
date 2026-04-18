/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.s3;

import org.opensearch.dataprepper.plugins.source.s3.configuration.S3ScanKeyPathOption;

import java.util.List;
import java.util.Map;

/**
 * Filters S3 objects by key using top-level include_prefix and exclude_suffix options.
 */
public class S3ObjectKeyFilter {

    private final Map<String, S3ScanKeyPathOption> filters;

    public S3ObjectKeyFilter(final Map<String, S3ScanKeyPathOption> filters) {
        this.filters = filters;
    }

    /**
     * Returns true if the object key matches the filters for the given bucket,
     * or if no filters are configured for the bucket.
     */
    public boolean isKeyMatchingFilters(final String bucketName, final String objectKey) {
        if (filters == null || filters.isEmpty()) {
            return true;
        }

        final S3ScanKeyPathOption keyPathOption = filters.get(bucketName);
        if (keyPathOption == null) {
            return true;
        }

        final List<String> includePrefixes = keyPathOption.getS3scanIncludePrefixOptions();
        if (includePrefixes != null && !includePrefixes.isEmpty()
                && includePrefixes.stream().noneMatch(objectKey::startsWith)) {
            return false;
        }

        final List<String> excludeSuffixes = keyPathOption.getS3ScanExcludeSuffixOptions();
        if (excludeSuffixes != null && !excludeSuffixes.isEmpty()
                && excludeSuffixes.stream().anyMatch(objectKey::endsWith)) {
            return false;
        }

        return true;
    }
}
