/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import java.net.MalformedURLException;
import java.net.URL;

public class SqsQueueUrl {
    private final URL queueUrl;
    private final String accountId;

    private SqsQueueUrl(final URL queueUrl) throws MalformedURLException {
        this.queueUrl = queueUrl;
        final String path = queueUrl.getPath();

        if (path.isEmpty())
            throw new MalformedURLException();

        final String[] pathParts = path.split("/");
        if (pathParts.length < 3)
            throw new MalformedURLException();

        accountId = pathParts[1];
    }

    public String getAccountId() {
        return accountId;
    }

    public static SqsQueueUrl parse(final String queueUrl) throws MalformedURLException {
        return new SqsQueueUrl(new URL(queueUrl));
    }
}
