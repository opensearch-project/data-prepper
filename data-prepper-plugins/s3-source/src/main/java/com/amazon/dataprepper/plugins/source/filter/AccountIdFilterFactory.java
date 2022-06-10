/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.filter;

import com.amazon.dataprepper.plugins.source.S3SourceConfig;
import com.amazon.dataprepper.plugins.source.SqsUrl;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class AccountIdFilterFactory implements FilterConfigFactory {
    @Override
    public Optional<S3EventFilter> createFilter(final S3SourceConfig s3SourceConfig) {
        final S3EventFilter accountIdFilter;
        if (s3SourceConfig.getAccountIds() != null && !s3SourceConfig.getAccountIds().isEmpty()) {
            accountIdFilter = createFilter(s3SourceConfig.getAccountIds());
        } else if (!s3SourceConfig.isAllowAnyAccountId()) {
            final String queueUrl = s3SourceConfig.getSqsOptions().getSqsUrl();
            final String accountId;
            try {
                accountId = SqsUrl.parse(queueUrl).getAccountId();
            } catch (final MalformedURLException e) {
                throw new IllegalArgumentException("queueUrl is invalid");
            }
            accountIdFilter = createFilter(Collections.singletonList(accountId));
        } else {
            accountIdFilter = null;
        }
        return Optional.ofNullable(accountIdFilter);
    }

    private static S3EventFilter createFilter(final List<String> accountIds) {
        return new FieldContainsS3EventFilter<>(
                record -> record.getS3().getBucket().getOwnerIdentity().getPrincipalId(),
                accountIds);
    }
}
