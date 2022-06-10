/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Creates an {@link S3EventFilter} from a configuration. This filter will filter out
 * S3 events which should not be processed by the S3 source.
 */
public class ConfigFilterConfigFactory {

    private final List<FilterConfigFactory> delegateFactories;

    public ConfigFilterConfigFactory() {
        this(Arrays.asList(
                new AccountIdFilterFactory(),
                new BucketFilterFactory(),
                new ObjectCreatedFilter.Factory()
        ));
    }

    ConfigFilterConfigFactory(final List<FilterConfigFactory> delegateFactories) {
        this.delegateFactories = delegateFactories;
    }

    public S3EventFilter createFilter(final S3SourceConfig s3SourceConfig) {
        final List<S3EventFilter> filters = delegateFactories.stream()
                .map(filterConfigFactory -> filterConfigFactory.createFilter(s3SourceConfig))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        return new S3EventFilterChain(filters);
    }
}
