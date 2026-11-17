/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultGcpCredentialsSupplierTest {
    private static final GcpCredentialsOptions PLUGIN_KEY_OPTIONS = GcpCredentialsOptions.builder()
            .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withClientEmail("plugin@p.iam.gserviceaccount.com")
            .withPrivateKey("key").build();
    private static final GcpCredentialsOptions DEFAULT_ADC_OPTIONS = GcpCredentialsOptions.builder()
            .withAuthMode(GcpAuthMode.ADC).withProjectId("default-project").build();

    @Mock
    private GoogleCredentialsFactory factory;

    @Mock
    private GoogleCredentials credentials;

    @Mock
    private GoogleCredentials applicationDefaultCredentials;

    private DefaultGcpCredentialsSupplier createObjectUnderTest(final GcpCredentialsOptions defaultOptions,
                                                                final ApplicationDefaultCredentialsLoader loader) {
        return new DefaultGcpCredentialsSupplier(defaultOptions, factory, new CredentialsCache(), loader);
    }

    private DefaultGcpCredentialsSupplier createObjectUnderTest(final GcpCredentialsOptions defaultOptions) {
        return createObjectUnderTest(defaultOptions, () -> applicationDefaultCredentials);
    }

    @Test
    void getCredentials_with_plugin_mode_uses_plugin_options() {
        when(factory.create(PLUGIN_KEY_OPTIONS)).thenReturn(credentials);

        assertThat(createObjectUnderTest(DEFAULT_ADC_OPTIONS).getCredentials(PLUGIN_KEY_OPTIONS), sameInstance(credentials));
    }

    @Test
    void getCredentials_without_plugin_mode_uses_default_options() {
        when(factory.create(DEFAULT_ADC_OPTIONS)).thenReturn(credentials);

        assertThat(createObjectUnderTest(DEFAULT_ADC_OPTIONS).getCredentials(GcpCredentialsOptions.builder().build()),
                sameInstance(credentials));
    }

    @Test
    void getCredentials_without_any_mode_falls_back_to_adc_with_plugin_scopes_and_project() {
        when(factory.create(any())).thenReturn(credentials);
        final GcpCredentialsOptions pluginOptions =
                GcpCredentialsOptions.builder().withScopes(List.of("scope-a")).withProjectId("plugin-project").build();

        createObjectUnderTest(GcpCredentialsOptions.builder().withProjectId("default-project").build()).getCredentials(pluginOptions);

        final ArgumentCaptor<GcpCredentialsOptions> captor = ArgumentCaptor.forClass(GcpCredentialsOptions.class);
        verify(factory).create(captor.capture());
        assertThat(captor.getValue(), equalTo(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC)
                .withScopes(List.of("scope-a")).withProjectId("plugin-project").build()));
    }

    @Test
    void getCredentials_with_null_options_and_no_default_falls_back_to_adc() {
        when(factory.create(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).build())).thenReturn(credentials);

        assertThat(createObjectUnderTest(null).getCredentials(null), sameInstance(credentials));
    }

    @Test
    void getCredentials_caches_by_resolved_options() {
        when(factory.create(PLUGIN_KEY_OPTIONS)).thenReturn(credentials);
        final DefaultGcpCredentialsSupplier objectUnderTest = createObjectUnderTest(null);

        objectUnderTest.getCredentials(PLUGIN_KEY_OPTIONS);
        objectUnderTest.getCredentials(PLUGIN_KEY_OPTIONS);

        verify(factory, times(1)).create(PLUGIN_KEY_OPTIONS);
    }

    @Test
    void getDefaultProjectId_prefers_default_options_project() {
        assertThat(createObjectUnderTest(DEFAULT_ADC_OPTIONS, () -> {
            throw new IllegalStateException("ADC must not be loaded");
        }).getDefaultProjectId(), equalTo(Optional.of("default-project")));
    }

    @Test
    void getDefaultProjectId_without_default_project_uses_adc_project() {
        when(applicationDefaultCredentials.getProjectId()).thenReturn("adc-project");

        assertThat(createObjectUnderTest(GcpCredentialsOptions.builder().build()).getDefaultProjectId(),
                equalTo(Optional.of("adc-project")));
    }

    @Test
    void getDefaultProjectId_without_default_options_and_adc_without_project_returns_empty() {
        when(applicationDefaultCredentials.getProjectId()).thenReturn(null);

        assertThat(createObjectUnderTest(null).getDefaultProjectId(), equalTo(Optional.empty()));
    }

    @Test
    void getDefaultProjectId_when_adc_unavailable_returns_empty() {
        assertThat(createObjectUnderTest(null, () -> {
            throw new IOException("no ADC");
        }).getDefaultProjectId(), equalTo(Optional.empty()));
    }
}
