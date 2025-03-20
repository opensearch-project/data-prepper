/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.ml.processor.util;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessor;
import org.opensearch.dataprepper.plugins.ml.processor.MLProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.*;
import software.amazon.awssdk.regions.Region;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.ml.processor.client.S3ClientFactory.convertToCredentialsOptions;

public class MlCommonRequester {
    private static final Aws4Signer signer;
    private static final Logger LOG = LoggerFactory.getLogger(MLProcessor.class);
    private static HttpClientExecutor httpClientExecutor = new SdkHttpClientExecutor();

    static {
        signer = Aws4Signer.create();
    }

    @VisibleForTesting
    static void setHttpClientExecutor(HttpClientExecutor executor) {
        httpClientExecutor = executor;
    }

    public static void sendRequestToMLCommons(String payload, MLProcessorConfig mlProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        String host = mlProcessorConfig.getHostUrl();
        String modelId = mlProcessorConfig.getModelId();
        String path = "/_plugins/_ml/models/" + modelId + "/" + mlProcessorConfig.getActionType().getMlCommonsActionValue();
        String url = host + path;
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(
                mlProcessorConfig.getAwsAuthenticationOptions());
        final Region region = mlProcessorConfig.getAwsAuthenticationOptions().getAwsRegion();
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(
                awsCredentialsOptions);

        RequestBody requestBody = RequestBody.fromString(payload);
        SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create(url))
                .contentStreamProvider(requestBody.contentStreamProvider())
                .putHeader("content-type", "application/json")
                .build();

        HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                .request(signRequest(request, region, awsCredentialsProvider))
                .contentStreamProvider(request.contentStreamProvider().orElse(null))
                .build();

        executeHttpRequest(executeRequest);
    }

    private static void executeHttpRequest(HttpExecuteRequest executeRequest) {
        try {
            HttpExecuteResponse response = httpClientExecutor.execute(executeRequest);

            handleHttpResponse(response);
        } catch (Exception e) {  // TODO: catch different exceptions and retry
            throw new RuntimeException("Failed to execute HTTP request using the ML Commons model", e);
        }
    }

    private static void handleHttpResponse(HttpExecuteResponse response) throws IOException {
        int statusCode = response.httpResponse().statusCode();
        String modelResponse = response.responseBody().map(MlCommonRequester::readStream).orElse("No response");

        System.out.println("Response Code: " + statusCode);
        System.out.println("Response Body: " + modelResponse);

        if (statusCode != 200) {
            throw new RuntimeException("Request failed with status code: " + statusCode);
        }
    }

    private static String readStream(AbortableInputStream stream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining());
        } catch (IOException e) {
            throw new RuntimeException("Error reading response body", e);
        }
    }

    private static SdkHttpFullRequest signRequest(SdkHttpFullRequest request, Region region, AwsCredentialsProvider awsCredentialsProvider) {
        try {
            AwsCredentials credentials = awsCredentialsProvider.resolveCredentials();

            String signingName = "es";
            Aws4SignerParams params = Aws4SignerParams
                    .builder()
                    .awsCredentials(credentials)
                    .signingName(signingName)
                    .signingRegion(region)
                    .build();

            return signer.sign(request, params);
        } catch (Exception e) {
            LOG.error("Failed to sign request due to credential retrieval error", e);
            throw new RuntimeException("Unable to sign AWS request", e);
        }
    }
}
