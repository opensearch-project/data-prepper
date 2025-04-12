/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;

import java.io.ByteArrayInputStream;

/**
 * Responsible for sending signed OTLP Protobuf trace data to AWS X-Ray OTLP endpoint.
 * This class uses AWS SDK's HTTP client to send the request signed using AWS SigV4.
 */
public class XRayOtlpHttpSender implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XRayOtlpHttpSender.class);

    private final SigV4Signer signer;
    private final SdkHttpClient httpClient;

    /**
     * Constructs a new OtlpHttpSender.
     *
     * @param signer     The SigV4Signer used to sign HTTP requests.
     * @param httpClient The AWS SDK HTTP client used to send signed requests.
     */
    public XRayOtlpHttpSender(final SigV4Signer signer, final SdkHttpClient httpClient) {
        this.signer = signer;
        this.httpClient = httpClient;
    }

    /**
     * Signs and sends the given OTLP-encoded span payload to the X-Ray OTLP endpoint.
     *
     * @param payload The OTLP Protobuf payload to send.
     */
    public void send(final byte[] payload) {
        try {
            final SdkHttpFullRequest signedRequest = signer.signRequest(payload);

            final HttpExecuteRequest httpRequest = HttpExecuteRequest.builder()
                    .request(signedRequest)
                    .contentStreamProvider(() -> new ByteArrayInputStream(payload))
                    .build();

            final HttpExecuteResponse response = httpClient.prepareRequest(httpRequest).call();
            final int status = response.httpResponse().statusCode();

            if (status >= 200 && status < 300) {
                LOG.info("Successfully sent OTLP data to AWS X-Ray. Status: {}", status);
            } else {
                LOG.warn("Failed to send OTLP data to AWS X-Ray. Status: {}", status);
            }
        } catch (final Exception e) {
            LOG.error("Error sending OTLP data to AWS X-Ray", e);
        }
    }

    /**
     * Closes the underlying HTTP client to release resources.
     */
    @Override
    public void close() {
        httpClient.close();
    }
}
