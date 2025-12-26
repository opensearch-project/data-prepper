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
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.lenient;

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
        assertEquals(4, config.getNumberOfWorkers());
        assertEquals(0, config.getLookBackMinutes());
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

    @Test
    void testNegativeDurationRange() throws Exception {
        Duration negativeDuration = Duration.ofDays(-1);
        setField(config, "range", negativeDuration);

        assertEquals(0, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_withMinuteRange() throws Exception {
        Duration fifteenMinutes = Duration.ofMinutes(15);
        setField(config, "range", fifteenMinutes);

        // getLookBackHours should return 0 for sub-hour range
        assertEquals(0, config.getLookBackHours());
        // getLookBackMinutes should return 15
        assertEquals(15, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_with30MinuteRange() throws Exception {
        Duration thirtyMinutes = Duration.ofMinutes(30);
        setField(config, "range", thirtyMinutes);

        // getLookBackHours should return 0 for sub-hour range
        assertEquals(0, config.getLookBackHours());
        // getLookBackMinutes should return 30
        assertEquals(30, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_with45MinuteRange() throws Exception {
        Duration fortyFiveMinutes = Duration.ofMinutes(45);
        setField(config, "range", fortyFiveMinutes);

        // getLookBackHours should return 0 for sub-hour range
        assertEquals(0, config.getLookBackHours());
        // getLookBackMinutes should return 45
        assertEquals(45, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_withHourRange() throws Exception {
        Duration twoHours = Duration.ofHours(2);
        setField(config, "range", twoHours);

        assertEquals(2, config.getLookBackHours());
        assertEquals(120, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_withDayRange() throws Exception {
        Duration oneDay = Duration.ofDays(1);
        setField(config, "range", oneDay);

        assertEquals(24, config.getLookBackHours());
        assertEquals(1440, config.getLookBackMinutes());
    }

    @Test
    void testGetLookBackMinutes_withZeroRange() throws Exception {
        Duration zeroDuration = Duration.ZERO;
        setField(config, "range", zeroDuration);

        assertEquals(0, config.getLookBackHours());
        assertEquals(0, config.getLookBackMinutes());
    }

    @Test
    void testDefaultDurationValues() {
        assertEquals(Duration.ofDays(30), config.getDurationToGiveUpRetry());
        assertEquals(Duration.ofDays(1), config.getDurationToDelayRetry());
    }
}
