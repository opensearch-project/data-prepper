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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
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
    private SimpleMeterRegistry meterRegistry;
    private String pipelineName;
    private String metricPrefix;

    @BeforeEach
    void setUp() {
        stsClient = mock(StsClient.class);
        httpClient = mock(HttpClient.class);
        meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);
        // Globally-unique pipeline name: PluginMetrics writes to the static Metrics.globalRegistry
        // whose meters outlive an add/removeRegistry cycle, so a name shared with another test class
        // would resolve a stale removed meter.
        pipelineName = "test-pipeline-" + UUID.randomUUID();
        metricPrefix = pipelineName + ".kafka.";
    }

    @AfterEach
    void tearDown() {
        Metrics.removeRegistry(meterRegistry);
        meterRegistry.clear();
        meterRegistry.close();
    }

    private AzureFederatedTokenProvider providerWith(final StsClient sts, final HttpClient http) {
        return providerWith(sts, http, null);
    }

    private AzureFederatedTokenProvider providerWith(final StsClient sts, final HttpClient http,
                                                     final KafkaSourceAuthMetrics authMetrics) {
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));
        return new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                Collections.emptyMap(), supplier, provider -> sts, http, authMetrics);
    }

    private KafkaSourceAuthMetrics newAuthMetrics() {
        return new KafkaSourceAuthMetrics(PluginMetrics.fromNames("kafka", pipelineName));
    }

    private double failureCount(final String cause) {
        final Counter counter = meterRegistry.find(metricPrefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_FAILURES)
                .tag(KafkaSourceAuthMetrics.ERROR_TYPE_TAG, cause).counter();
        return counter == null ? 0.0 : counter.count();
    }

    // Dynamic token body (random access_token + expiry) so tests prove behavior rather than pinning
    // to constant literals, per the project's dynamic-test-values convention.
    private String successBody() {
        return "{\"access_token\":\"" + UUID.randomUUID()
                + "\",\"expires_in\":" + ThreadLocalRandom.current().nextInt(60, 86_400) + "}";
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
    void getToken_whenStsRequestFails_throwsGenericStsMessageWithStatusAndErrorCode() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                        .message("AccessDenied").build());

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> providerWith(stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("STS request failed during azure_federated authentication"));
        assertThat(e.getMessage(), containsString("403"));
        assertThat(e.getMessage(), containsString("AccessDenied"));
    }

    @Test
    void getToken_whenOutboundFederationDisabled_throwsFederationDisabledMessage() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(OutboundWebIdentityFederationDisabledException.builder().message("disabled").build());

        final RuntimeException e = assertThrows(RuntimeException.class,
                () -> providerWith(stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("outbound web identity federation feature is not enabled"));
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
    void getToken_whileCached_returnsDistinctInstancesWithSameValueAndLifetime() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final String accessToken = UUID.randomUUID().toString();
        final int expiresIn = ThreadLocalRandom.current().nextInt(1, 86_400);
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"" + accessToken + "\",\"expires_in\":" + expiresIn + "}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(response);
        final AzureFederatedTokenProvider provider = providerWith(stsClient, httpClient);

        final AzureFederatedOAuthBearerToken first = provider.getToken();
        final AzureFederatedOAuthBearerToken second = provider.getToken();

        // Kafka's OAUTHBEARER re-login identifies tokens on the Subject by instance identity;
        // reusing one instance makes commit() a no-op and the subsequent logout() empties the
        // Subject. Each call must therefore return a new instance.
        assertThat(second, not(sameInstance(first)));
        // The exchange must not repeat while cached, so the fresh instance carries the same value
        // and the same absolute lifetime rather than a lifetime recomputed from "now".
        verify(httpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
        assertThat(second.value(), equalTo(accessToken));
        assertThat(second.value(), equalTo(first.value()));
        assertThat(second.lifetimeMs(), equalTo(first.lifetimeMs()));
        assertThat(second.startTimeMs(), equalTo(first.startTimeMs()));
    }

    @Test
    void getToken_resolvesCredentialsThroughSupplier_withRoleAndRegion() throws Exception {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, successBody());
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
        final HttpResponse<String> response = httpResponse(200, successBody());
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
        final String accessToken = UUID.randomUUID().toString();
        final HttpResponse<String> response =
                httpResponse(200, "{\"access_token\":\"" + accessToken + "\",\"expires_in\":3599}");
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

        assertThat(token.value(), equalTo(accessToken));
    }

    @Test
    void getToken_whenStsRequestFailsWithNullRole_doesNotLeakNullLiteral() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                        .message("AccessDenied").build());
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        final RuntimeException e = assertThrows(RuntimeException.class, () ->
                new AzureFederatedTokenProvider(REGION, null, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                        Collections.emptyMap(), supplier, provider -> stsClient, httpClient).getToken());

        assertThat(e.getMessage(), not(containsString("null")));
        assertThat(e.getMessage(), containsString("STS request failed during azure_federated authentication"));
    }

    @Test
    void getToken_whenUnrecognizedExceptionThrown_surfacesGenericMessage() {
        // simulate a non-SDK failure surfacing at the credential-materialization point
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(new RuntimeException("com.amazonaws delegation assume failed"));
        final AwsCredentialsSupplier supplier = mock(AwsCredentialsSupplier.class);
        when(supplier.getProvider(any(AwsCredentialsOptions.class)))
                .thenReturn(mock(AwsCredentialsProvider.class));

        final RuntimeException e = assertThrows(RuntimeException.class, () ->
                new AzureFederatedTokenProvider(REGION, STS_ROLE_ARN, TOKEN_ENDPOINT, CLIENT_ID, SCOPE,
                        Collections.emptyMap(), supplier, provider -> stsClient, httpClient).getToken());

        assertThat(e.getMessage(), containsString("Unexpected failure during azure_federated authentication"));
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

    @Test
    void getToken_onSuccess_incrementsRefreshCountAndSetsExpiry() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, successBody());
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        providerWith(stsClient, httpClient, authMetrics).getToken();

        final Counter refreshCount =
                meterRegistry.find(metricPrefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_SUCCESS).counter();
        assertThat(refreshCount, notNullValue());
        assertThat(refreshCount.count(), equalTo(1.0));
        final io.micrometer.core.instrument.Gauge expiry =
                meterRegistry.find(metricPrefix + KafkaSourceAuthMetrics.TIME_TO_TOKEN_REFRESH).gauge();
        assertThat(expiry.value(), greaterThan(0.0));
    }

    @Test
    void getToken_whileCached_incrementsRefreshCountOnlyOnActualMint() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, successBody());
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();
        final AzureFederatedTokenProvider provider = providerWith(stsClient, httpClient, authMetrics);

        provider.getToken();
        provider.getToken(); // served from cache, must NOT be counted as a refresh

        assertThat(meterRegistry.find(metricPrefix + KafkaSourceAuthMetrics.TOKEN_REFRESH_SUCCESS).counter().count(),
                equalTo(1.0));
    }

    @Test
    void getToken_on403_incrementsFailuresTaggedAccessDenied() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                        .message("AccessDenied").build());
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        assertThrows(RuntimeException.class, () -> providerWith(stsClient, httpClient, authMetrics).getToken());

        assertThat(failureCount(KafkaSourceAuthMetrics.CAUSE_AWS_STS_ACCESS_DENIED), equalTo(1.0));
    }

    @Test
    void getToken_onNonAccessDeniedStsError_incrementsFailuresTaggedStsError() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(500)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalServerError").build())
                        .message("STS internal error").build());
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        assertThrows(RuntimeException.class, () -> providerWith(stsClient, httpClient, authMetrics).getToken());

        assertThat(failureCount(KafkaSourceAuthMetrics.CAUSE_AWS_STS_ERROR), equalTo(1.0));
    }

    @Test
    void getToken_whenFederationDisabled_incrementsFailuresTaggedFederationDisabled() {
        when(stsClient.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(OutboundWebIdentityFederationDisabledException.builder().message("disabled").build());
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        assertThrows(RuntimeException.class, () -> providerWith(stsClient, httpClient, authMetrics).getToken());

        assertThat(failureCount(KafkaSourceAuthMetrics.CAUSE_AWS_OUTBOUND_FEDERATION_DISABLED), equalTo(1.0));
    }

    @Test
    void getToken_onNon200_incrementsFailuresTaggedAadsts() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(401,
                "{\"error\":\"invalid_client\",\"error_description\":\"AADSTS700016: not found\"}");
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        assertThrows(RuntimeException.class, () -> providerWith(stsClient, httpClient, authMetrics).getToken());

        assertThat(failureCount(KafkaSourceAuthMetrics.CAUSE_AZURE_TOKEN_EXCHANGE_REJECTED), equalTo(1.0));
    }

    @Test
    void getToken_onHttpIoException_incrementsFailuresTaggedNetwork() throws IOException, InterruptedException {
        stubWebIdentityTokenSuccess();
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenThrow(new IOException("connection reset"));
        final KafkaSourceAuthMetrics authMetrics = newAuthMetrics();

        assertThrows(RuntimeException.class, () -> providerWith(stsClient, httpClient, authMetrics).getToken());

        assertThat(failureCount(KafkaSourceAuthMetrics.CAUSE_NETWORK), equalTo(1.0));
    }

    @Test
    void getToken_withNullMetrics_doesNotThrowOnSuccessOrFailure() throws IOException, InterruptedException {
        // success path with null metrics
        stubWebIdentityTokenSuccess();
        final HttpResponse<String> response = httpResponse(200, successBody());
        when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn(response);
        providerWith(stsClient, httpClient, null).getToken();

        // failure path with null metrics: the original actionable error must survive, i.e. the null
        // metrics must NOT be dereferenced (an NPE from a missing null-guard also extends
        // RuntimeException, so assertThrows(RuntimeException) alone would not catch that regression).
        final StsClient failing = mock(StsClient.class);
        when(failing.getWebIdentityToken(any(java.util.function.Consumer.class)))
                .thenThrow(StsException.builder().statusCode(403)
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                        .message("AccessDenied").build());
        final RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> providerWith(failing, httpClient, null).getToken());
        assertThat(thrown, not(instanceOf(NullPointerException.class)));
        assertThat(thrown.getMessage(), containsString("STS request failed during azure_federated authentication"));
    }
}
