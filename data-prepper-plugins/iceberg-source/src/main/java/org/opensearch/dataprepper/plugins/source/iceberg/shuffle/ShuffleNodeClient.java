/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ClientFactoryBuilder;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP client for pulling shuffle index and data from remote nodes.
 */
public class ShuffleNodeClient {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleNodeClient.class);
    private static final int MAX_RETRIES = 3;
    private static final Duration RESPONSE_TIMEOUT = Duration.ofSeconds(30);

    private final String scheme;
    private final int port;
    private final ClientFactory clientFactory;
    private final Map<String, WebClient> clientCache = new ConcurrentHashMap<>();

    public ShuffleNodeClient(final ShuffleConfig config, final Certificate certificate) {
        this.scheme = config.isSsl() ? "https" : "http";
        this.port = config.getServerPort();
        this.clientFactory = buildClientFactory(config, certificate);
    }

    public long[] pullIndex(final String nodeAddress, final String snapshotId, final String taskId) throws Exception {
        final byte[] body = executeWithRetry(nodeAddress,
                String.format("/shuffle/%s/%s/index", snapshotId, taskId),
                Duration.ofSeconds(10),
                "index from " + nodeAddress);
        final ByteBuffer buf = ByteBuffer.wrap(body);
        final long[] offsets = new long[body.length / Long.BYTES];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = buf.getLong();
        }
        return offsets;
    }

    public byte[] pullData(final String nodeAddress, final String snapshotId, final String taskId,
                           final long offset, final int length) throws Exception {
        return executeWithRetry(nodeAddress,
                String.format("/shuffle/%s/%s/data?offset=%d&length=%d", snapshotId, taskId, offset, length),
                RESPONSE_TIMEOUT,
                "data from " + nodeAddress);
    }

    public void requestCleanup(final String nodeAddress, final String snapshotId) {
        try {
            final WebClient client = getClient(nodeAddress);
            client.execute(RequestHeaders.of(HttpMethod.DELETE, String.format("/shuffle/%s", snapshotId)))
                    .aggregate()
                    .thenAccept(response -> {
                        if (!response.status().equals(HttpStatus.OK)) {
                            LOG.warn("Remote cleanup failed for snapshot {} on {}: status={}", snapshotId, nodeAddress, response.status());
                        }
                    })
                    .exceptionally(error -> {
                        LOG.warn("Remote cleanup failed for snapshot {} on {}", snapshotId, nodeAddress, error);
                        return null;
                    });
        } catch (final Exception e) {
            LOG.warn("Remote cleanup failed for snapshot {} on {}", snapshotId, nodeAddress, e);
        }
    }

    public long[] collectPartitionSizes(final ShuffleStorage shuffleStorage,
                                         final String snapshotId,
                                         final List<String> taskIds,
                                         final List<String> nodeAddresses,
                                         final int numPartitions) {
        final long[] sizes = new long[numPartitions];
        for (int i = 0; i < taskIds.size(); i++) {
            final String taskId = taskIds.get(i);
            final String nodeAddress = nodeAddresses.get(i);
            try {
                final long[] offsets;
                if (isLocalAddress(nodeAddress)) {
                    try (var reader = shuffleStorage.createReader(snapshotId, taskId)) {
                        offsets = reader.readIndex();
                    }
                } else {
                    offsets = pullIndex(nodeAddress, snapshotId, taskId);
                }
                for (int p = 0; p < numPartitions && p + 1 < offsets.length; p++) {
                    sizes[p] += offsets[p + 1] - offsets[p];
                }
            } catch (final Exception e) {
                LOG.warn("Failed to read index for task {} from node {}, skipping for coalesce", taskId, nodeAddress, e);
            }
        }
        return sizes;
    }

    public static boolean isLocalAddress(final String address) {
        try {
            final InetAddress inetAddress = InetAddress.getByName(address);
            if (inetAddress.isAnyLocalAddress() || inetAddress.isLoopbackAddress()) {
                return true;
            }
            return NetworkInterface.getByInetAddress(inetAddress) != null;
        } catch (final Exception e) {
            return false;
        }
    }

    public static String resolveLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to resolve local host name", e);
        }
    }

    private WebClient getClient(final String nodeAddress) {
        return clientCache.computeIfAbsent(nodeAddress, addr ->
                WebClient.builder(String.format("%s://%s:%d", scheme, addr, port))
                        .factory(clientFactory)
                        .responseTimeout(RESPONSE_TIMEOUT)
                        .build());
    }

    private byte[] executeWithRetry(final String nodeAddress, final String path,
                                     final Duration timeout, final String description) throws Exception {
        final WebClient client = getClient(nodeAddress);
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                final AggregatedHttpResponse response = client.get(path).aggregate().join();
                if (response.status().equals(HttpStatus.OK)) {
                    try (var content = response.content()) {
                        return content.array();
                    }
                }
                LOG.warn("HTTP pull failed for {}: status={} attempt={}/{}", description, response.status(), attempt, MAX_RETRIES);
            } catch (final Exception e) {
                LOG.warn("HTTP pull failed for {}: attempt={}/{}", description, attempt, MAX_RETRIES, e);
            }
            if (attempt < MAX_RETRIES) {
                Thread.sleep(1000L * attempt);
            }
        }
        throw new RuntimeException("Failed to pull " + description + " after " + MAX_RETRIES + " retries");
    }

    private static ClientFactory buildClientFactory(final ShuffleConfig config, final Certificate certificate) {
        if (!config.isSsl()) {
            return ClientFactory.ofDefault();
        }

        final ClientFactoryBuilder builder = ClientFactory.builder();

        if (config.isSslInsecureDisableVerification()) {
            builder.tlsNoVerify();
        } else if (certificate != null) {
            builder.tlsCustomizer(sslContextBuilder -> sslContextBuilder.trustManager(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8))));
        }

        if (config.isSslClientAuth() && certificate != null) {
            builder.tlsCustomizer(sslContextBuilder -> sslContextBuilder.keyManager(
                    new ByteArrayInputStream(certificate.getCertificate().getBytes(StandardCharsets.UTF_8)),
                    new ByteArrayInputStream(certificate.getPrivateKey().getBytes(StandardCharsets.UTF_8))));
        }

        return builder.build();
    }
}
