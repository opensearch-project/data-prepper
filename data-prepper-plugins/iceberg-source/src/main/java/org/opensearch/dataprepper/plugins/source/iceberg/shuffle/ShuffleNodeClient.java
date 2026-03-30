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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;

/**
 * Shared HTTP client utilities for pulling shuffle index and data from remote nodes.
 * Used by both LeaderScheduler (index collection for coalesce) and ChangelogWorker (data pull for SHUFFLE_READ).
 */
public class ShuffleNodeClient {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleNodeClient.class);
    private static final int MAX_RETRIES = 3;

    private final HttpClient httpClient;
    private final String scheme;
    private final int port;

    public ShuffleNodeClient(final ShuffleConfig config) {
        this.httpClient = buildHttpClient(config);
        this.scheme = config.isSsl() ? "https" : "http";
        this.port = config.getServerPort();
    }

    public long[] pullIndex(final String nodeAddress, final String snapshotId, final String taskId) throws Exception {
        final byte[] body = executeWithRetry(
                String.format("%s://%s:%d/shuffle/%s/%s/index", scheme, nodeAddress, port, snapshotId, taskId),
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
        return executeWithRetry(
                String.format("%s://%s:%d/shuffle/%s/%s/data?offset=%d&length=%d",
                        scheme, nodeAddress, port, snapshotId, taskId, offset, length),
                Duration.ofSeconds(30),
                "data from " + nodeAddress);
    }

    private byte[] executeWithRetry(final String url, final Duration timeout, final String description) throws Exception {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                final HttpResponse<byte[]> response = httpClient.send(
                        HttpRequest.newBuilder(URI.create(url)).GET().timeout(timeout).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() == 200) {
                    return response.body();
                }
                LOG.warn("HTTP pull failed for {}: status={} attempt={}/{}", description, response.statusCode(), attempt, MAX_RETRIES);
            } catch (final Exception e) {
                LOG.warn("HTTP pull failed for {}: attempt={}/{}", description, attempt, MAX_RETRIES, e);
            }
            if (attempt < MAX_RETRIES) {
                Thread.sleep(1000L * attempt);
            }
        }
        throw new RuntimeException("Failed to pull " + description + " after " + MAX_RETRIES + " retries");
    }

    /**
     * Requests a remote node to delete shuffle files for a snapshot.
     */
    public void requestCleanup(final String nodeAddress, final String snapshotId) {
        try {
            final URI uri = URI.create(String.format("%s://%s:%d/shuffle/%s",
                    scheme, nodeAddress, port, snapshotId));
            httpClient.sendAsync(
                    HttpRequest.newBuilder(uri).DELETE().timeout(Duration.ofSeconds(10)).build(),
                    HttpResponse.BodyHandlers.discarding())
                    .thenAccept(response -> {
                        if (response.statusCode() != 200) {
                            LOG.warn("Remote cleanup failed for snapshot {} on {}: status={}", snapshotId, nodeAddress, response.statusCode());
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

    /**
     * Collects partition sizes from all nodes by reading index files.
     * Local tasks are read from disk, remote tasks are fetched via HTTP.
     */
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

    private static HttpClient buildHttpClient(final ShuffleConfig config) {
        final HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10));
        if (config.isSsl() && config.isSslInsecureDisableVerification()) {
            try {
                final TrustManager[] trustAllCerts = new TrustManager[]{
                        new X509TrustManager() {
                            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
                        }
                };
                final SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
                builder.sslContext(sslContext);
            } catch (final Exception e) {
                throw new RuntimeException("Failed to configure insecure SSL context", e);
            }
        }
        return builder.build();
    }
}
