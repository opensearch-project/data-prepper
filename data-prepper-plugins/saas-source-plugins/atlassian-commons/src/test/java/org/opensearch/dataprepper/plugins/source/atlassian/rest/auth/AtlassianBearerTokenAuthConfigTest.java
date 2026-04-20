/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.atlassian.AtlassianSourceConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AuthenticationConfig;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AtlassianBearerTokenAuthConfigTest {

    @Mock
    private AtlassianSourceConfig sourceConfig;

    @Mock
    private AuthenticationConfig authenticationConfig;

    @Test
    void testGetUrl() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://confluence.internal.com");
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBearerToken()).thenReturn("test-token");

        AtlassianBearerTokenAuthConfig config = new AtlassianBearerTokenAuthConfig(sourceConfig);
        assertThat(config.getUrl(), equalTo("https://confluence.internal.com/"));
    }

    @Test
    void testGetUrlWithTrailingSlash() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://confluence.internal.com/");
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBearerToken()).thenReturn("test-token");

        AtlassianBearerTokenAuthConfig config = new AtlassianBearerTokenAuthConfig(sourceConfig);
        assertThat(config.getUrl(), equalTo("https://confluence.internal.com/"));
    }

    @Test
    void testGetBearerToken() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://confluence.internal.com");
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBearerToken()).thenReturn("my-pat-token");

        AtlassianBearerTokenAuthConfig config = new AtlassianBearerTokenAuthConfig(sourceConfig);
        assertThat(config.getBearerToken(), equalTo("my-pat-token"));
    }

    @Test
    void testRenewCredentialsIsNoOp() {
        when(sourceConfig.getAccountUrl()).thenReturn("https://confluence.internal.com");
        when(sourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBearerToken()).thenReturn("test-token");

        AtlassianBearerTokenAuthConfig config = new AtlassianBearerTokenAuthConfig(sourceConfig);
        assertDoesNotThrow(config::renewCredentials);
    }
}
