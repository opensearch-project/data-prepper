/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence.rest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.BasicConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.Oauth2Config;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.BasicAuthInterceptor;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.CustomRestTemplateConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.OAuth2RequestInterceptor;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.ConfluenceSourceConfig;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.InterceptingClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.OAUTH2;

@ExtendWith(MockitoExtension.class)
class CustomRestTemplateConfigTest {

    private CustomRestTemplateConfig config;

    @Mock
    private ConfluenceSourceConfig mockSourceConfig;

    @Mock
    private AtlassianAuthConfig mockAuthConfig;

    @Mock
    private BasicConfig mockBasicConfig;

    @Mock
    private Oauth2Config mockOauth2Config;

    @Mock
    private PluginConfigVariable accessTokenPluginConfigVariable;

    @Mock
    private PluginConfigVariable refreshTokenPluginConfigVariable;

    @Mock
    private AuthenticationConfig mockAuthenticationConfig;

    private static Stream<Arguments> provideAuthTypeAndExpectedInterceptorType() {
        return Stream.of(
                Arguments.of(OAUTH2, OAuth2RequestInterceptor.class),
                Arguments.of(BASIC, BasicAuthInterceptor.class),
                Arguments.of("Default", BasicAuthInterceptor.class),
                Arguments.of(null, BasicAuthInterceptor.class)
        );
    }

    @BeforeEach
    void setUp() {
        config = new CustomRestTemplateConfig();
    }

    @ParameterizedTest
    @MethodSource("provideAuthTypeAndExpectedInterceptorType")
    void testBasicAuthRestTemplateWithOAuth2(String authType, Class interceptorClassType) {
        when(mockSourceConfig.getAuthType()).thenReturn(authType);
        lenient().when(mockSourceConfig.getAuthenticationConfig()).thenReturn(mockAuthenticationConfig);
        lenient().when(mockAuthenticationConfig.getOauth2Config()).thenReturn(mockOauth2Config);
        lenient().when(mockOauth2Config.getAccessToken()).thenReturn(accessTokenPluginConfigVariable);
        lenient().when(mockOauth2Config.getRefreshToken()).thenReturn(refreshTokenPluginConfigVariable);
        lenient().when(accessTokenPluginConfigVariable.getValue()).thenReturn("accessToken");
        lenient().when(mockOauth2Config.getClientId()).thenReturn("clientId");
        lenient().when(mockOauth2Config.getClientSecret()).thenReturn("clientSecret");
        lenient().when(mockAuthenticationConfig.getBasicConfig()).thenReturn(mockBasicConfig);
        lenient().when(mockBasicConfig.getUsername()).thenReturn("username");
        lenient().when(mockBasicConfig.getPassword()).thenReturn("password");

        RestTemplate restTemplate = config.basicAuthRestTemplate(mockSourceConfig, mockAuthConfig);
        assertNotNull(restTemplate);
        assertInstanceOf(InterceptingClientHttpRequestFactory.class, restTemplate.getRequestFactory());
        List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
        assertEquals(1, interceptors.size());
        assertInstanceOf(interceptorClassType, interceptors.get(0));
    }

}

