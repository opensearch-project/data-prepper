/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.connector;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.annotation.ConnectorExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.exception.MLBatchJobException;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.HttpClientExecutor;
import org.opensearch.dataprepper.plugins.ml_inference.processor.util.SdkHttpClientExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.ml_inference.processor.client.S3ClientFactory.convertToCredentialsOptions;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.connector.ConnectorProtocols.AWS_SIGV4;

/**
 * Executes connector actions against AWS services using SigV4 signing, analogous to
 * {@code AwsConnectorExecutor} in ml-commons.
 *
 * <p>Annotated with {@link ConnectorExecutor @ConnectorExecutor("aws_sigv4")} so that the
 * runtime can route any connector with {@code "protocol": "aws_sigv4"} to this executor.
 * The constructor accepts a {@link Connector} (the typed {@link AwsConnector} instance
 * stored in {@link BuiltInConnectors}) and casts it internally.
 */
@ConnectorExecutor(AWS_SIGV4)
public class AwsConnectorExecutor extends AbstractConnectorExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AwsConnectorExecutor.class);
    private static final Aws4Signer SIGNER = Aws4Signer.create();

    private final AwsConnector connector;
    private final MLProcessorConfig config;
    private final AwsCredentialsSupplier awsCredentialsSupplier;
    private final HttpClientExecutor httpClientExecutor;

    /**
     * @param connector the {@link AwsConnector} definition — passed as the {@link Connector}
     *                  interface so callers need not downcast from {@link BuiltInConnectors}
     */
    public AwsConnectorExecutor(final Connector connector,
                                final MLProcessorConfig config,
                                final AwsCredentialsSupplier awsCredentialsSupplier) {
        this(connector, config, awsCredentialsSupplier, new SdkHttpClientExecutor());
    }

    AwsConnectorExecutor(final Connector connector,
                         final MLProcessorConfig config,
                         final AwsCredentialsSupplier awsCredentialsSupplier,
                         final HttpClientExecutor httpClientExecutor) {
        this.connector = (AwsConnector) connector;
        this.config = config;
        this.awsCredentialsSupplier = awsCredentialsSupplier;
        this.httpClientExecutor = httpClientExecutor;
    }

    @Override
    public AwsConnector getConnector() {
        return connector;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Builds the SDK request, signs it with AWS SigV4 using the service name from
     * {@code connector.parameters.service_name}, then executes it synchronously.
     */
    @Override
    protected void sendRequest(final ConnectorAction action,
                               final String url,
                               final String payload,
                               final Map<String, String> merged) {
        final SdkHttpMethod method = SdkHttpMethod.fromValue(action.getMethod());

        final SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .method(method)
                .uri(URI.create(url));

        action.getHeaders().forEach(requestBuilder::putHeader);

        if (method != SdkHttpMethod.GET && !payload.isEmpty()) {
            final RequestBody body = RequestBody.fromString(payload, StandardCharsets.UTF_8);
            requestBuilder.contentStreamProvider(body.contentStreamProvider());
        }

        final SdkHttpFullRequest request = requestBuilder.build();

        final String serviceName = connector.getServiceName();
        final String regionStr = merged.getOrDefault("region",
                config.getAwsAuthenticationOptions().getAwsRegion().id());
        LOG.debug("Signing SigV4 request: service={}, region={}, uri={}", serviceName, regionStr, url);
        final SdkHttpFullRequest signedRequest = signRequest(request, serviceName, Region.of(regionStr));

        final HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                .request(signedRequest)
                .contentStreamProvider(request.contentStreamProvider().orElse(null))
                .build();

        executeHttpRequest(executeRequest, action.getActionType());
    }

    private SdkHttpFullRequest signRequest(final SdkHttpFullRequest request,
                                           final String serviceName,
                                           final Region region) {
        final AwsCredentialsOptions credentialsOptions = convertToCredentialsOptions(
                config.getAwsAuthenticationOptions());
        final AwsCredentialsProvider credentialsProvider = awsCredentialsSupplier.getProvider(credentialsOptions);
        try {
            final AwsCredentials credentials = credentialsProvider.resolveCredentials();
            final Aws4SignerParams params = Aws4SignerParams.builder()
                    .awsCredentials(credentials)
                    .signingName(serviceName)
                    .signingRegion(region)
                    .build();
            return SIGNER.sign(request, params);
        } catch (final Exception e) {
            LOG.error("Failed to sign {} request to {}", serviceName, request.getUri(), e);
            throw new RuntimeException("Unable to sign AWS request for service: " + serviceName, e);
        }
    }

    private void executeHttpRequest(final HttpExecuteRequest executeRequest, final String action) {
        final HttpExecuteResponse response;
        try {
            response = httpClientExecutor.execute(executeRequest);
        } catch (final IOException e) {
            LOG.error("IOException executing {} action: {}", action, e.getMessage());
            throw new MLBatchJobException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Failed to execute " + action + " request due to IO issue: " + e.getMessage());
        } catch (final Exception e) {
            LOG.error("Unexpected error executing {} action", action, e);
            throw new MLBatchJobException(HttpURLConnection.HTTP_INTERNAL_ERROR,
                    "Unexpected error executing " + action + " request: " + e.getMessage());
        }
        handleHttpResponse(response, action);
    }

    private void handleHttpResponse(final HttpExecuteResponse response, final String action) {
        final int statusCode = response.httpResponse().statusCode();
        final String responseBody = response.responseBody().map(this::readStream).orElse("No response");

        if (statusCode == 429) {
            LOG.warn("Remote service throttled {} request (429): {}", action, responseBody);
            throw new MLBatchJobException(statusCode,
                    action + " request was throttled: " + responseBody);
        } else if (statusCode >= 400 && statusCode < 500) {
            LOG.error("Remote service client error on {} ({}): {}", action, statusCode, responseBody);
            throw new MLBatchJobException(statusCode,
                    "Client error " + statusCode + " on " + action + ": " + responseBody);
        } else if (statusCode >= 500) {
            LOG.error("Remote service server error on {} ({}): {}", action, statusCode, responseBody);
            throw new MLBatchJobException(statusCode,
                    "Server error " + statusCode + " on " + action + ": " + responseBody);
        } else if (statusCode != 200) {
            LOG.error("Unexpected status {} on {}: {}", statusCode, action, responseBody);
            throw new MLBatchJobException(statusCode,
                    "Unexpected status code " + statusCode + " on " + action);
        }

        LOG.info("{} request succeeded: {}", action, responseBody);
    }

    private String readStream(final AbortableInputStream stream) {
        try (final BufferedReader reader = new BufferedReader(
                new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining());
        } catch (final IOException e) {
            LOG.error("Failed to read remote service response body", e);
            throw new RuntimeException("Error reading response body", e);
        }
    }
}
