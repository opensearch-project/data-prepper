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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetWebIdentityTokenResponse;
import software.amazon.awssdk.services.sts.model.OutboundWebIdentityFederationDisabledException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

public class AzureFederatedTokenProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AzureFederatedTokenProvider.class);

    private static final String STS_AUDIENCE = "api://AzureADTokenExchange";
    private static final String SIGNING_ALGORITHM = "RS256";
    private static final int JWT_DURATION_SECONDS = 300;
    private static final String GRANT_TYPE = "client_credentials";
    private static final String CLIENT_ASSERTION_TYPE =
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    private static final long SKEW_BUFFER_MS = 30_000L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String region;
    private final String stsRoleArn;
    private final String tokenEndpoint;
    private final String clientId;
    private final String scope;
    private final Supplier<StsClient> stsClientSupplier;
    private final HttpClient httpClient;
    // Null on paths without a PluginMetrics handle (buffer/admin); metric sites are then no-ops.
    private final KafkaSourceAuthMetrics authMetrics;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile CachedToken cached;
    private volatile StsClient stsClient;

    public AzureFederatedTokenProvider(final String region, final String stsRoleArn, final String tokenEndpoint,
                                       final String clientId, final String scope,
                                       final Map<String, String> stsHeaderOverrides,
                                       final AwsCredentialsSupplier awsCredentialsSupplier) {
        this(region, stsRoleArn, tokenEndpoint, clientId, scope, stsHeaderOverrides, awsCredentialsSupplier, null);
    }

    public AzureFederatedTokenProvider(final String region, final String stsRoleArn, final String tokenEndpoint,
                                       final String clientId, final String scope,
                                       final Map<String, String> stsHeaderOverrides,
                                       final AwsCredentialsSupplier awsCredentialsSupplier,
                                       final KafkaSourceAuthMetrics authMetrics) {
        this(region, stsRoleArn, tokenEndpoint, clientId, scope, stsHeaderOverrides, awsCredentialsSupplier,
                provider -> StsClient.builder()
                        .region(Region.of(region))
                        .credentialsProvider(provider)
                        .build(),
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build(),
                authMetrics);
    }

    AzureFederatedTokenProvider(final String region, final String stsRoleArn, final String tokenEndpoint,
                                final String clientId, final String scope,
                                final Map<String, String> stsHeaderOverrides,
                                final AwsCredentialsSupplier awsCredentialsSupplier,
                                final Function<AwsCredentialsProvider, StsClient> stsClientFactory,
                                final HttpClient httpClient) {
        this(region, stsRoleArn, tokenEndpoint, clientId, scope, stsHeaderOverrides, awsCredentialsSupplier,
                stsClientFactory, httpClient, null);
    }

    AzureFederatedTokenProvider(final String region, final String stsRoleArn, final String tokenEndpoint,
                                final String clientId, final String scope,
                                final Map<String, String> stsHeaderOverrides,
                                final AwsCredentialsSupplier awsCredentialsSupplier,
                                final Function<AwsCredentialsProvider, StsClient> stsClientFactory,
                                final HttpClient httpClient,
                                final KafkaSourceAuthMetrics authMetrics) {
        this.region = region;
        this.stsRoleArn = stsRoleArn;
        this.tokenEndpoint = tokenEndpoint;
        this.clientId = clientId;
        this.scope = scope;
        this.stsClientSupplier =
                () -> stsClientFactory.apply(baseCredentials(region, stsRoleArn, stsHeaderOverrides, awsCredentialsSupplier));
        this.httpClient = httpClient;
        this.authMetrics = authMetrics;
    }

    String getRegion() {
        return region;
    }

    String getStsRoleArn() {
        return stsRoleArn;
    }

    String getTokenEndpoint() {
        return tokenEndpoint;
    }

    String getClientId() {
        return clientId;
    }

    String getScope() {
        return scope;
    }

    public AzureFederatedOAuthBearerToken getToken() {
        final CachedToken current = cached;
        if (current != null && current.isValid()) {
            return current.newTokenInstance();
        }
        lock.lock();
        try {
            if (cached != null && cached.isValid()) {
                return cached.newTokenInstance();
            }
            final AzureFederatedOAuthBearerToken token;
            try {
                token = exchange();
            } catch (final RuntimeException e) {
                if (authMetrics != null) {
                    authMetrics.recordFailure(classifyFailure(e));
                }
                throw e;
            }
            cached = new CachedToken(token);
            if (authMetrics != null) {
                // Headroom to the proactive re-mint deadline (expiry minus skew), not raw expiry.
                authMetrics.recordRefresh(token.lifetimeMs() - SKEW_BUFFER_MS);
            }
            return token;
        } finally {
            lock.unlock();
        }
    }

    private AzureFederatedOAuthBearerToken exchange() {
        return postToAzure(mintAwsJwt());
    }

    // Walk the cause chain to a bounded cause tag; anything unmatched is CAUSE_OTHER.
    private static String classifyFailure(final RuntimeException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof OutboundWebIdentityFederationDisabledException) {
                return KafkaSourceAuthMetrics.CAUSE_AWS_OUTBOUND_FEDERATION_DISABLED;
            }
            if (t instanceof StsException) {
                final StsException stsException = (StsException) t;
                final String errorCode = errorCodeOf(stsException);
                return errorCode != null && errorCode.contains("AccessDenied")
                        ? KafkaSourceAuthMetrics.CAUSE_AWS_STS_ACCESS_DENIED
                        : KafkaSourceAuthMetrics.CAUSE_AWS_STS_ERROR;
            }
            if (t instanceof AzureTokenExchangeException) {
                return KafkaSourceAuthMetrics.CAUSE_AZURE_TOKEN_EXCHANGE_REJECTED;
            }
            if (t instanceof IOException || t instanceof InterruptedException) {
                return KafkaSourceAuthMetrics.CAUSE_NETWORK;
            }
        }
        return KafkaSourceAuthMetrics.CAUSE_OTHER;
    }

    private static String errorCodeOf(final StsException e) {
        return e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
    }

    private StsClient stsClient() {
        if (stsClient == null) {
            stsClient = stsClientSupplier.get();
        }
        return stsClient;
    }

    public void close() {
        lock.lock();
        try {
            if (stsClient != null) {
                stsClient.close();
                stsClient = null;
            }
        } finally {
            lock.unlock();
        }
    }

    private static AwsCredentialsProvider baseCredentials(final String region, final String stsRoleArn,
                                                          final Map<String, String> stsHeaderOverrides,
                                                          final AwsCredentialsSupplier awsCredentialsSupplier) {
        if (awsCredentialsSupplier == null) {
            throw new RuntimeException("azure_federated requires an AWS credentials extension to be configured");
        }
        return awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder()
                .withStsRoleArn(stsRoleArn)
                .withRegion(Region.of(region))
                .withStsHeaderOverrides(stsHeaderOverrides)
                .build());
    }

    private String mintAwsJwt() {
        try {
            final GetWebIdentityTokenResponse response = stsClient().getWebIdentityToken(builder -> builder
                    .audience(STS_AUDIENCE)
                    .signingAlgorithm(SIGNING_ALGORITHM)
                    .durationSeconds(JWT_DURATION_SECONDS));
            return response.webIdentityToken();
        } catch (final OutboundWebIdentityFederationDisabledException e) {
            LOG.error("The outbound web identity federation feature is not enabled for the account.", e);
            throw new RuntimeException("The outbound web identity federation feature is not enabled for the account.", e);
        } catch (final StsException e) {
            final String errorCode = errorCodeOf(e);
            LOG.error("STS request failed during azure_federated authentication (HTTP {}, errorCode {}).",
                    e.statusCode(), errorCode, e);
            throw new RuntimeException("STS request failed during azure_federated authentication (HTTP "
                    + e.statusCode() + ", errorCode " + errorCode + ")", e);
        } catch (final Exception e) {
            LOG.error("Unexpected failure during azure_federated authentication.", e);
            throw new RuntimeException("Unexpected failure during azure_federated authentication", e);
        }
    }

    private AzureFederatedOAuthBearerToken postToAzure(final String awsJwt) {
        final String form = "grant_type=" + encode(GRANT_TYPE)
                + "&client_id=" + encode(clientId)
                + "&client_assertion_type=" + encode(CLIENT_ASSERTION_TYPE)
                + "&client_assertion=" + encode(awsJwt)
                + "&scope=" + encode(scope);
        try {
            final HttpRequest request = HttpRequest.newBuilder(URI.create(tokenEndpoint))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(form))
                    .build();
            final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                final String aadsts = extractAadstsError(response.body());
                LOG.error("Azure token exchange was rejected (HTTP {}, {}).", response.statusCode(), aadsts);
                throw new AzureTokenExchangeException(response.statusCode(), aadsts);
            }
            final JsonNode json = OBJECT_MAPPER.readTree(response.body());
            final String accessToken = json.get("access_token").asText();
            final long expiresIn = json.get("expires_in").asLong();
            return new AzureFederatedOAuthBearerToken(accessToken, expiresIn, scope, clientId);
        } catch (final IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Azure token exchange request failed to " + tokenEndpoint, e);
        }
    }

    private String extractAadstsError(final String body) {
        try {
            final JsonNode json = OBJECT_MAPPER.readTree(body);
            final String error = json.path("error").asText("");
            final String description = json.path("error_description").asText("");
            return (error + " " + description).trim();
        } catch (final IOException e) {
            return "";
        }
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    // Dedicated type so the failure metric classifies the Azure non-200 path by instanceof rather than
    // by message text (which is then free to change). The AADSTS code is surfaced, not interpreted.
    static final class AzureTokenExchangeException extends RuntimeException {
        AzureTokenExchangeException(final int statusCode, final String aadsts) {
            super("Azure token exchange was rejected (HTTP " + statusCode + ", " + aadsts + ")");
        }
    }

    private final class CachedToken {
        private final AzureFederatedOAuthBearerToken token;
        private final long validUntilMs;

        private CachedToken(final AzureFederatedOAuthBearerToken token) {
            this.token = token;
            this.validUntilMs = token.lifetimeMs() - SKEW_BUFFER_MS;
        }

        private boolean isValid() {
            return System.currentTimeMillis() < validUntilMs;
        }

        /**
         * Returns a distinct token instance carrying the cached credential's value and its
         * original absolute lifetime. Kafka identifies tokens on the Subject by instance identity
         * during re-login, so each callback must receive a new instance - while preserving the
         * lifetime so refresh scheduling does not drift forward on every call.
         */
        private AzureFederatedOAuthBearerToken newTokenInstance() {
            return new AzureFederatedOAuthBearerToken(token);
        }
    }
}
