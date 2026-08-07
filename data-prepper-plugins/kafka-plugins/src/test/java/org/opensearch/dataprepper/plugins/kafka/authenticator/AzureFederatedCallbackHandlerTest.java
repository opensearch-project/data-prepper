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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureFederatedCallbackHandlerTest {

    private static final String REGION = "us-east-1";
    private static final String STS_ROLE_ARN = "arn:aws:iam::123456789012:role/eh-federation";
    private static final String TOKEN_ENDPOINT =
            "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/v2.0/token";
    private static final String CLIENT_ID = "11111111-1111-1111-1111-111111111111";
    private static final String SCOPE = "https://my-namespace.servicebus.windows.net/.default";

    private Map<String, String> jaasOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("azureFederatedRegion", REGION);
        options.put("azureFederatedStsRoleArn", STS_ROLE_ARN);
        options.put("azureFederatedTokenEndpoint", TOKEN_ENDPOINT);
        options.put("azureFederatedClientId", CLIENT_ID);
        options.put("azureFederatedScope", SCOPE);
        return options;
    }

    private List<AppConfigurationEntry> jaasEntries(final Map<String, String> options) {
        return List.of(new AppConfigurationEntry(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options));
    }

    @AfterEach
    void resetSupplier() {
        AwsCredentialsSupplierProvider.getInstance().set(null);
    }

    @Test
    void configure_buildsProviderFromJaasOptionKeys() {
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler();

        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasEntries(jaasOptions()));

        final AzureFederatedTokenProvider provider = handler.getTokenProvider();
        assertThat(provider.getRegion(), equalTo(REGION));
        assertThat(provider.getStsRoleArn(), equalTo(STS_ROLE_ARN));
        assertThat(provider.getTokenEndpoint(), equalTo(TOKEN_ENDPOINT));
        assertThat(provider.getClientId(), equalTo(CLIENT_ID));
        assertThat(provider.getScope(), equalTo(SCOPE));
    }

    @Test
    void configure_readsSupplierFromSingleton_andPassesItToProvider() {
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        final RuntimeException sentinel = new RuntimeException("supplier-was-consulted");
        when(supplier.getProvider(any(AwsCredentialsOptions.class))).thenThrow(sentinel);
        AwsCredentialsSupplierProvider.getInstance().set(supplier);
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler();

        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasEntries(jaasOptions()));
        // provider construction is lazy; the supplier is consulted when the token is first minted
        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> handler.getTokenProvider().getToken());

        assertThat(thrown.getCause().getMessage(), containsString("supplier-was-consulted"));
        verify(supplier).getProvider(any(AwsCredentialsOptions.class));
    }

    @Test
    void configure_withStsHeaderOverridesOption_decodesAndPassesThemToProvider() throws Exception {
        final Map<String, String> stsHeaderOverrides = Map.of(
                "x-amz-source-arn", "arn:aws:osis:us-east-1:123456789012:pipeline/p",
                "x-amz-source-account", "123456789012");
        final String encoded = Base64.getEncoder().encodeToString(
                new ObjectMapper().writeValueAsString(stsHeaderOverrides).getBytes(StandardCharsets.UTF_8));
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));
        AwsCredentialsSupplierProvider.getInstance().set(supplier);
        final Map<String, String> options = jaasOptions();
        options.put("azureFederatedStsHeaderOverrides", encoded);
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler();

        handler.configure(Map.of(), OAuthBearerLoginModule.OAUTHBEARER_MECHANISM, jaasEntries(options));
        try {
            handler.getTokenProvider().getToken();
        } catch (final RuntimeException ignored) {
            // token minting hits STS/network after credential resolution; the supplier is still consulted first
        }

        final ArgumentCaptor<AwsCredentialsOptions> captor =
                ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(supplier).getProvider(captor.capture());
        assertThat(captor.getValue().getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void configure_withWrongMechanism_throwsIllegalArgumentException() {
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler();

        assertThrows(IllegalArgumentException.class,
                () -> handler.configure(Map.of(), "SCRAM-SHA-512", jaasEntries(jaasOptions())));
    }

    @Test
    void handle_withOAuthBearerTokenCallback_setsTokenFromProvider() throws Exception {
        final AzureFederatedTokenProvider provider = mock(AzureFederatedTokenProvider.class);
        final AzureFederatedOAuthBearerToken token =
                new AzureFederatedOAuthBearerToken("access-token", 3600L, SCOPE, CLIENT_ID);
        when(provider.getToken()).thenReturn(token);
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler(provider);
        final OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();

        handler.handle(new Callback[] {callback});

        assertThat(callback.token(), sameInstance(token));
    }

    @Test
    void handle_withUnsupportedCallback_throwsUnsupportedCallbackException() {
        final AzureFederatedTokenProvider provider = mock(AzureFederatedTokenProvider.class);
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler(provider);
        final Callback unsupported = new NameCallback("prompt");

        assertThrows(UnsupportedCallbackException.class,
                () -> handler.handle(new Callback[] {unsupported}));
    }

    @Test
    void close_withProvider_closesTokenProvider() {
        final AzureFederatedTokenProvider provider = mock(AzureFederatedTokenProvider.class);
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler(provider);

        handler.close();

        verify(provider).close();
    }

    @Test
    void close_withoutConfiguredProvider_isNoOp() {
        final AzureFederatedCallbackHandler handler = new AzureFederatedCallbackHandler();

        handler.close();
    }
}
