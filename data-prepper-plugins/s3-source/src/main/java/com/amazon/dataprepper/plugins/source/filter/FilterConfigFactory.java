/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;

import java.util.Optional;

interface FilterConfigFactory {
    Optional<S3EventFilter> createFilter(S3SourceConfig config);
}
