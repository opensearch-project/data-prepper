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
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.gcp.api.GcpAuthMode;
import org.opensearch.dataprepper.gcp.api.GcpCredentialsOptions;

import java.io.IOException;
import java.net.URI;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GoogleCredentialsFactoryTest {
    private static final String CLIENT_EMAIL = "sa@my-project.iam.gserviceaccount.com";

    @Mock
    private GoogleCredentials applicationDefaultCredentials;

    @Mock
    private GoogleCredentials scopedCredentials;

    private static String generatePkcs8PrivateKey() throws NoSuchAlgorithmException {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        final String body = Base64.getMimeEncoder(64, "\n".getBytes())
                .encodeToString(generator.generateKeyPair().getPrivate().getEncoded());
        return "-----BEGIN PRIVATE KEY-----\n" + body + "\n-----END PRIVATE KEY-----\n";
    }

    private GoogleCredentialsFactory createObjectUnderTest() {
        return new GoogleCredentialsFactory(() -> applicationDefaultCredentials, new TokenUriValidator());
    }

    @Test
    void create_adc_returns_scoped_application_default_credentials() {
        when(applicationDefaultCredentials.createScoped(List.of(GcpCredentialsOptions.DEFAULT_SCOPE))).thenReturn(scopedCredentials);

        final GoogleCredentials result = createObjectUnderTest()
                .create(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).build());

        assertThat(result, sameInstance(scopedCredentials));
    }

    @Test
    void create_without_mode_uses_adc() {
        when(applicationDefaultCredentials.createScoped(List.of("scope-a"))).thenReturn(scopedCredentials);

        final GoogleCredentials result = createObjectUnderTest()
                .create(GcpCredentialsOptions.builder().withScopes(List.of("scope-a")).build());

        assertThat(result, sameInstance(scopedCredentials));
    }

    @Test
    void create_adc_when_loader_fails_throws_naming_mode() {
        final GoogleCredentialsFactory objectUnderTest = new GoogleCredentialsFactory(() -> {
            throw new IOException("no ADC");
        }, new TokenUriValidator());

        final GcpCredentialsException exception = assertThrows(GcpCredentialsException.class,
                () -> objectUnderTest.create(GcpCredentialsOptions.builder().withAuthMode(GcpAuthMode.ADC).build()));

        assertThat(exception.getMessage(), containsString("ADC/Workload Identity"));
        assertThat(exception.getMessage(), containsString("no ADC"));
    }

    @Test
    void create_service_account_key_builds_service_account_credentials() throws NoSuchAlgorithmException {
        final GcpCredentialsOptions options = GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY)
                .withClientEmail(CLIENT_EMAIL)
                .withPrivateKey(generatePkcs8PrivateKey())
                .withProjectId("my-project")
                .withScopes(List.of("scope-a"))
                .build();

        final GoogleCredentials result = createObjectUnderTest().create(options);

        assertThat(result, instanceOf(ServiceAccountCredentials.class));
        final ServiceAccountCredentials serviceAccountCredentials = (ServiceAccountCredentials) result;
        assertThat(serviceAccountCredentials.getClientEmail(), equalTo(CLIENT_EMAIL));
        assertThat(serviceAccountCredentials.getProjectId(), equalTo("my-project"));
        assertThat(List.copyOf(serviceAccountCredentials.getScopes()), equalTo(List.of("scope-a")));
        assertThat(serviceAccountCredentials.getTokenServerUri(), equalTo(URI.create(GcpCredentialsOptions.DEFAULT_TOKEN_URI)));
    }

    @Test
    void create_service_account_key_with_invalid_key_throws_naming_mode_without_key_material() {
        final String invalidKey = "-----BEGIN PRIVATE KEY-----\nbm90IGEga2V5\n-----END PRIVATE KEY-----\n";
        final GcpCredentialsOptions options = GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withClientEmail(CLIENT_EMAIL).withPrivateKey(invalidKey).build();
        final GoogleCredentialsFactory objectUnderTest = createObjectUnderTest();

        final GcpCredentialsException exception = assertThrows(GcpCredentialsException.class, () -> objectUnderTest.create(options));

        assertThat(exception.getMessage(), containsString("service-account key"));
        assertThat(exception.getMessage(), not(containsString("bm90IGEga2V5")));
    }

    @Test
    void create_service_account_key_with_disallowed_token_uri_throws() throws NoSuchAlgorithmException {
        final GcpCredentialsOptions options = GcpCredentialsOptions.builder()
                .withAuthMode(GcpAuthMode.SERVICE_ACCOUNT_KEY).withClientEmail(CLIENT_EMAIL)
                .withPrivateKey(generatePkcs8PrivateKey()).withTokenUri("https://evil.example.com/token").build();
        final GoogleCredentialsFactory objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.create(options));
    }

    @Test
    void default_constructor_creates_factory() {
        assertThat(new GoogleCredentialsFactory(), instanceOf(GoogleCredentialsFactory.class));
    }
}
