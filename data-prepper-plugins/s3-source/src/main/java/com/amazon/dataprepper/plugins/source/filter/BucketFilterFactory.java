/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;

import java.util.List;
import java.util.Optional;

class BucketFilterFactory implements FilterConfigFactory {
    @Override
    public Optional<S3EventFilter> createFilter(final S3SourceConfig s3SourceConfig) {
        final List<String> buckets = s3SourceConfig.getBuckets();
        if (buckets == null || buckets.isEmpty())
            return Optional.empty();

        return Optional.of(new FieldContainsS3EventFilter<>(
                record -> record.getS3().getBucket().getName(),
                buckets));
    }
}
