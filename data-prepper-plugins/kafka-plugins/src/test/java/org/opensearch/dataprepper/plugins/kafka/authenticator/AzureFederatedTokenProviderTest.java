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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenResponse;
import software.amazon.awssdk.services.sts.model.OutboundWebIdentityFederationDisabledException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AzureFederatedTokenProviderTest {

    private static final String REGION = "us-east-1";
    private static final String STS_ROLE_ARN = "arn:aws:iam::123456789012:role/eh-federation";
    private static final String TOKEN_ENDPOINT =
            "https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000/oauth2/v2.0/token";
    private static final String CLIENT_ID = "11111111-1111-1111-1111-111111111111";
    private static final String SCOPE = "https://my-namespace.servicebus.windows.net/.default";
    private static final String AWS_JWT = "aws.web.identity.jwt";

    private StsClient stsClient;
    private HttpClient httpClient;

    @BeforeEach
    void setUp() {
        stsClient = mock(StsClient.class);
        httpClient = mock(HttpClient.class);
    }

    private AzureFederatedTokenProvider providerWith(final StsClient sts, final HttpClient http) {
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));
        return new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                Collections.emptyMap(), supplier, provider -> sts, http);
    }

    private void stubWebIdentityTokenSuccess() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenReturn(GetWebIdentityTokenResponse.builder().webIdentityToken(AWS_JWT).build());
    }

    @SuppressWarnings("unchecked")
    private HttpResponse<String> httpResponse(final int status, final String body) {
        final HttpResponse<String> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.body()).thenReturn(body);
        return response;
    }

    @Test
    void getToken_onSuccess_parsesAccessTokenAndExpiry() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"azure-access-token\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);

        final AzureFederatedOAuthBearerToken token = providerWith(stsClient, httpClient).getToken();

        assertThat(token.value(), equalTo("azure-access-token"));
        assertThat(token.lifetimeMs(), greaterThan(System.currentTimeMillis()));
        assertThat(token.principalName(), equalTo(CLIENT_ID));
    }

    @Test
    void getToken_onNon200_throwsWithHttpStatusAndAadstsCode() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(401,
                "{\"error\":\"invalid_client\",\"error_description\":\"AADSTS700016: Application not found\"}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> providerWith(stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("401"));
        assertThat(e.getMessage(), containsString("AADSTS700016"));
    }

    @Test
    void getToken_whenGetWebIdentityToken403_throwsAccessDeniedMessage() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403).message("AccessDenied").build());

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> providerWith(stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString(STS_ROLE_ARN));
        assertThat(e.getMessage(), containsString("sts:GetWebIdentityToken"));
    }

    @Test
    void getToken_whenOutboundFederationDisabled_throwsEnableFederationMessage() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(OutboundWebIdentityFederationDisabledException.builder().message("disabled").build());

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> providerWith(stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("Outbound"));
    }

    @Test
    void getToken_isSingleFlight_runsExchangeExactlyOnceUnderConcurrency() throws Exception {
        final AtomicInteger stsCalls = new AtomicInteger(0);
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenAnswer(invocation -> {
                    stsCalls.incrementAndGet();
                    return GetWebIdentityTokenResponse.builder().webIdentityToken(AWS_JWT).build();
                });
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"azure-access-token\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);

        final AzureFederatedTokenProvider provider = providerWith(stsClient, httpClient);
        final int threads = 8;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                start.await();
                return provider.getToken();
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS), equalTo(true));

        verify(stsClient, times(1)).getWebIdentityToken(any(java.util.function.Consumer.class));
        assertThat(stsCalls.get(), equalTo(1));
    }

    @Test
    void getToken_buildsStsClientOnce_acrossMultipleExchanges() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"azure-access-token\",\"expires_in\":0}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AtomicInteger supplierCalls = new AtomicInteger(0);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));
        final AzureFederatedTokenProvider provider = new AzureFederatedTokenProvider(
                REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE, Collections.emptyMap(), supplier,
                provider2 -> {
                    supplierCalls.incrementAndGet();
                    return stsClient;
                }, httpClient);

        provider.getToken();
        provider.getToken();

        assertThat(supplierCalls.get(), equalTo(1));
        verify(httpClient, times(2)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    void getToken_resolvesCredentialsThroughSupplier_withRoleAndRegion() throws Exception {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, "{\"access_token\":\"t\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                Collections.emptyMap(), supplier, provider -> stsClient, httpClient).getToken();

        final ArgumentCaptor<AwsCredentialsOptions> captor =
                ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(supplier).getProvider(captor.capture());
        assertThat(captor.getValue().getStsRoleArn(), equalTo(STS_ROLE_ARN));
        assertThat(captor.getValue().getRegion().id(), equalTo(REGION));
    }

    @Test
    void getToken_passesStsHeaderOverridesToSupplier() throws Exception {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, "{\"access_token\":\"t\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));
        final Map<String, String> stsHeaderOverrides = Map.of(
                "x-amz-source-arn", "arn:aws:osis:us-east-1:123456789012:pipeline/p",
                "x-amz-source-account", "123456789012");

        new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                stsHeaderOverrides, supplier, provider -> stsClient, httpClient).getToken();

        final ArgumentCaptor<AwsCredentialsOptions> captor =
                ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(supplier).getProvider(captor.capture());
        assertThat(captor.getValue().getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }

    @Test
    void getToken_withNullRole_stillResolvesThroughSupplierAndMints() throws Exception {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, "{\"access_token\":\"t\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        // null role is a valid outcome (role supplied out of band) — must NOT throw
        final AzureFederatedOAuthBearerToken token = new AzureFederatedTokenProvider(
                REGION, null, TOKEN_ENDPOINT, CLIENT_ID, SCOPE, Collections.emptyMap(), supplier,
                provider -> stsClient, httpClient)
                .getToken();

        assertThat(token.value(), equalTo("t"));
    }

    @Test
    void getToken_whenGetWebIdentityToken403WithNullRole_usesStableIdentifierNotNullLiteral() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403).message("AccessDenied").build());
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        final RuntimeException e = assertThrows(RuntimeException.class, () ->
                new AzureFederatedTokenProvider(REGION, null, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                        Collections.emptyMap(), supplier, provider -> stsClient, httpClient).getToken());

        assertThat(e.getMessage(), not(containsString("null")));
        assertThat(e.getMessage(), containsString("sts:GetWebIdentityToken"));
    }

    @Test
    void getToken_whenCredentialResolutionThrowsNonSdkException_surfacesActionableMessage() {
        // simulate a non-SDK failure surfacing at the credential-materialization point
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(new RuntimeException("com.amazonaws delegation assume failed"));
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        final RuntimeException e = assertThrows(RuntimeException.class, () ->
                new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                        Collections.emptyMap(), supplier, provider -> stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("AWS credential resolution failed"));
    }

    @Test
    void getToken_withNullSupplier_throwsExtensionRequired() {
        final RuntimeException e = assertThrows(RuntimeException.class, () ->
                new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                        Collections.emptyMap(), null).getToken());

        assertThat(e.getCause().getMessage(), containsString("requires an AWS credentials extension"));
    }

    @Test
    void close_afterStsClientCreated_closesStsClient() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"azure-access-token\",\"expires_in\":3599}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AzureFederatedTokenProvider provider = providerWith(stsClient, httpClient);
        provider.getToken();

        provider.close();

        verify(stsClient).close();
    }

    @Test
    void close_whenStsClientNeverCreated_isNoOp() {
        final AzureFederatedTokenProvider provider = providerWith(stsClient, httpClient);

        provider.close();

        verify(stsClient, never()).close();
    }
}
