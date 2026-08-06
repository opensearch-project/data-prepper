/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.authenticator;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

class AzureFederatedOAuthBearerTokenTest {

    private static final String TOKEN_VALUE = "eyJ0eXAiOiJKV1Qif-a-fake-access-token";
    private static final String SCOPE = "https://my-namespace.servicebus.windows.net/.default";
    private static final String PRINCIPAL = "11111111-1111-1111-1111-111111111111";

    @Test
    void lifetimeMs_isAbsoluteEpochExpiry_notTheRawDuration() {
        final long expiresInSeconds = 3600L;
        final long before = System.currentTimeMillis();

        final AzureFederatedOAuthBearerToken token =
                new AzureFederatedOAuthBearerToken(TOKEN_VALUE, expiresInSeconds, SCOPE, PRINCIPAL);

        final long after = System.currentTimeMillis();

        assertThat(token.lifetimeMs(), greaterThan(after));
        assertThat(token.lifetimeMs(), greaterThanOrEqualTo(before + expiresInSeconds * 1000L));
        assertThat(token.lifetimeMs(), lessThanOrEqualTo(after + expiresInSeconds * 1000L));
    }

    @Test
    void startTimeMs_isTheIssueInstant() {
        final long before = System.currentTimeMillis();

        final AzureFederatedOAuthBearerToken token =
                new AzureFederatedOAuthBearerToken(TOKEN_VALUE, 3600L, SCOPE, PRINCIPAL);

        final long after = System.currentTimeMillis();

        assertThat(token.startTimeMs(), greaterThanOrEqualTo(before));
        assertThat(token.startTimeMs(), lessThanOrEqualTo(after));
    }

    @Test
    void valueAndPrincipalAndScope_areReturnedVerbatim() {
        final AzureFederatedOAuthBearerToken token =
                new AzureFederatedOAuthBearerToken(TOKEN_VALUE, 3600L, SCOPE, PRINCIPAL);

        assertThat(token.value(), equalTo(TOKEN_VALUE));
        assertThat(token.principalName(), equalTo(PRINCIPAL));
        assertThat(token.scope(), contains(SCOPE));
    }

    @Test
    void scope_isEmptyWhenNull() {
        final AzureFederatedOAuthBearerToken token =
                new AzureFederatedOAuthBearerToken(TOKEN_VALUE, 3600L, null, PRINCIPAL);

        assertThat(token.scope().isEmpty(), equalTo(true));
    }
}
