/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.s3;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

public class SqsQueueUrl {
    private final String accountId;

    private SqsQueueUrl(final URL queueUrl) {
        final String path = queueUrl.getPath();

        if (path.isEmpty())
            throw new IllegalArgumentException("No path for the SQS queue URL.");

        final String[] pathParts = path.split("/");
        if (pathParts.length < 3)
            throw new IllegalArgumentException("Not enough path parts for the SQS queue URL.");

        accountId = pathParts[1];

        if(accountId.length() != 12) {
            throw new IllegalArgumentException("SQS queue URL has accountId of invalid length.");
        }
        if(!accountId.chars().allMatch(Character::isDigit)) {
            throw new IllegalArgumentException("SQS queue URL has accountId with invalid characters.");
        }
    }

    public String getAccountId() {
        return accountId;
    }

    public static SqsQueueUrl parse(final String queueUrl) throws MalformedURLException {
        Objects.requireNonNull(queueUrl);
        return new SqsQueueUrl(new URL(queueUrl));
    }
}
