/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.microsoft_office365;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigVariable;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.AuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.microsoft_office365.auth.Oauth2Config;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class Office365SourceConfigTest {
    private final String tenantId = "test-tenant-id";
    private final String clientId = "test-client-id";
    private final String clientSecret = "test-client-secret";

    @Mock
    private PluginConfigVariable mockClientId;

    @Mock
    private PluginConfigVariable mockClientSecret;

    @Mock
    private AuthenticationConfiguration mockAuthConfig;

    @Mock
    private Oauth2Config mockOauth2Config;

    private Office365SourceConfig config;

    @BeforeEach
    void setUp() throws Exception {
        // Set up lenient mocks to avoid UnnecessaryStubbingException
        lenient().when(mockClientId.getValue()).thenReturn(clientId);
        lenient().when(mockClientSecret.getValue()).thenReturn(clientSecret);
        lenient().when(mockOauth2Config.getClientId()).thenReturn(mockClientId);
        lenient().when(mockOauth2Config.getClientSecret()).thenReturn(mockClientSecret);
        lenient().when(mockAuthConfig.getOauth2()).thenReturn(mockOauth2Config);

        // Create config and inject mocked dependencies using reflection
        config = new Office365SourceConfig();
        setField(config, "tenantId", tenantId);
        setField(config, "authenticationConfiguration", mockAuthConfig);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    void testGetters() {
        assertEquals(tenantId, config.getTenantId());
        assertNotNull(config.getAuthenticationConfiguration());
        assertEquals(mockClientId, config.getAuthenticationConfiguration().getOauth2().getClientId());
        assertEquals(mockClientSecret, config.getAuthenticationConfiguration().getOauth2().getClientSecret());
    }

    @Test
    void testDefaultValues() {
        assertFalse(config.isAcknowledgments());
        assertEquals(7, config.getNumberOfWorkers());
    }

    @Test
    void testGetClientIdValue() {
        String actualClientId = (String) config.getAuthenticationConfiguration().getOauth2().getClientId().getValue();
        assertEquals(clientId, actualClientId);
    }

    @Test
    void testGetClientSecretValue() {
        String actualClientSecret = (String) config.getAuthenticationConfiguration().getOauth2().getClientSecret().getValue();
        assertEquals(clientSecret, actualClientSecret);
    }
}
