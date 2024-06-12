/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.data.generation;

import java.util.Random;
import java.util.UUID;

public class UserAgent {
    private static final UserAgent USER_AGENT = new UserAgent();
    private final Random random;

    private UserAgent() {
        random = new Random();
    }

    public static UserAgent getInstance() {
        return USER_AGENT;
    }

    public String userAgent() {
        final StringBuilder userAgentBuilder = new StringBuilder();

        buildBrowserPart(userAgentBuilder);
        userAgentBuilder.append(" (");

        buildDevicePart(userAgentBuilder);
        userAgentBuilder.append(" ");

        buildOsPart(userAgentBuilder);
        userAgentBuilder.append(")");

        return userAgentBuilder.toString();
    }

    private void buildOsPart(final StringBuilder userAgentBuilder) {
        userAgentBuilder.append(randomString());
        userAgentBuilder.append(" ");
        buildVersionString(userAgentBuilder);
    }

    private void buildDevicePart(final StringBuilder userAgentBuilder) {
        userAgentBuilder.append(randomString());
        userAgentBuilder.append(";");
    }

    private void buildBrowserPart(final StringBuilder userAgentBuilder) {
        userAgentBuilder.append(randomString());
        userAgentBuilder.append("/");
        buildVersionString(userAgentBuilder);
    }

    private void buildVersionString(final StringBuilder userAgentBuilder) {
        userAgentBuilder.append(random.nextInt(9) + 1);
        userAgentBuilder.append(".");
        userAgentBuilder.append(random.nextInt(30));
        userAgentBuilder.append(".");
        userAgentBuilder.append(random.nextInt(30));
    }

    private static String randomString() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }
}
