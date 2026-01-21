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
import java.time.Instant;

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
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        assertNotNull(lookBackDuration);
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

        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        assertNotNull(lookBackDuration);
    }

    @Test
    void testGetLookBackDuration_withMinuteRange() throws Exception {
        Duration fifteenMinutes = Duration.ofMinutes(15);
        setField(config, "range", fifteenMinutes);

        Instant now = Instant.now();
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        // Verify the duration is approximately 15 minutes before now (within 1 second tolerance)
        Duration actualDuration = Duration.between(lookBackDuration, now);
        assertEquals(15, actualDuration.toMinutes());
    }

    @Test
    void testGetLookBackDuration_with30MinuteRange() throws Exception {
        Duration thirtyMinutes = Duration.ofMinutes(30);
        setField(config, "range", thirtyMinutes);

        Instant now = Instant.now();
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        // Verify the duration is approximately 30 minutes before now
        Duration actualDuration = Duration.between(lookBackDuration, now);
        assertEquals(30, actualDuration.toMinutes());
    }

    @Test
    void testGetLookBackDuration_with45MinuteRange() throws Exception {
        Duration fortyFiveMinutes = Duration.ofMinutes(45);
        setField(config, "range", fortyFiveMinutes);

        Instant now = Instant.now();
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        // Verify the duration is approximately 45 minutes before now
        Duration actualDuration = Duration.between(lookBackDuration, now);
        assertEquals(45, actualDuration.toMinutes());
    }

    @Test
    void testGetLookBackDuration_withHourRange() throws Exception {
        Duration twoHours = Duration.ofHours(2);
        setField(config, "range", twoHours);

        Instant now = Instant.now();
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        Duration actualDuration = Duration.between(lookBackDuration, now);
        assertEquals(2, actualDuration.toHours());
        assertEquals(120, actualDuration.toMinutes());
    }

    @Test
    void testGetLookBackDuration_withDayRange() throws Exception {
        Duration oneDay = Duration.ofDays(1);
        setField(config, "range", oneDay);

        Instant now = Instant.now();
        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        Duration actualDuration = Duration.between(lookBackDuration, now);
        assertEquals(24, actualDuration.toHours());
        assertEquals(1440, actualDuration.toMinutes());
    }

    @Test
    void testGetLookBackDuration_withZeroRange() throws Exception {
        Duration zeroDuration = Duration.ZERO;
        setField(config, "range", zeroDuration);

        Instant lookBackDuration = config.getLookBackDuration(Instant.now());
        assertNotNull(lookBackDuration);
    }

    @Test
    void testDefaultDurationValues() {
        assertEquals(Duration.ofDays(30), config.getDurationToGiveUpRetry());
        assertEquals(Duration.ofDays(1), config.getDurationToDelayRetry());
    }
}
