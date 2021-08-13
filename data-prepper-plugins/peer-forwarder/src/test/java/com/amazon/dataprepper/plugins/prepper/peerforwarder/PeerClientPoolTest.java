/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper.peerforwarder;

import com.amazon.dataprepper.plugins.prepper.peerforwarder.certificate.model.Certificate;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertNotNull;

public class PeerClientPoolTest {
    private static final String VALID_ADDRESS = "10.10.10.5";
    private static final String LOCALHOST = "localhost";
    private static final int PORT = 21890;
    private static final File SSL_KEY_FILE = new File(
            PeerClientPoolTest.class.getClassLoader().getResource("test-key.key").getFile());
    private static final File SSL_CRT_FILE = new File(
            PeerClientPoolTest.class.getClassLoader().getResource("test-crt.crt").getFile());

    @Test
    public void testGetClientValidAddress() {
        PeerClientPool pool = PeerClientPool.getInstance();
        pool.setPort(PORT);

        TraceServiceGrpc.TraceServiceBlockingStub client = pool.getClient(VALID_ADDRESS);

        assertNotNull(client);
    }

    @Test
    public void testGetClientWithSSL() throws IOException {
        // Set up test server with SSL
        ServerBuilder sb = Server.builder();
        sb.service(GrpcService.builder()
                .addService(new TestPeerService())
                .build());
        sb.tls(SSL_CRT_FILE, SSL_KEY_FILE).https(PORT);

        try (Server server = sb.build()) {
            server.start();

            // Configure client pool
            PeerClientPool pool = PeerClientPool.getInstance();
            pool.setSsl(true);

            final Path certFilePath = Path.of(PeerClientPoolTest.class.getClassLoader().getResource("test-crt.crt").getPath());
            final String certAsString = Files.readString(certFilePath);
            final Certificate certificate = new Certificate(certAsString);
            pool.setCertificate(certificate);
            TraceServiceGrpc.TraceServiceBlockingStub client = pool.getClient(LOCALHOST);
            assertNotNull(client);

            // Call API should not throw exception
            client.export(ExportTraceServiceRequest.newBuilder().build());
        }
    }

    public static class TestPeerService extends TraceServiceGrpc.TraceServiceImplBase {
        @Override
        public void export(final ExportTraceServiceRequest request, final StreamObserver<ExportTraceServiceResponse> responseObserver) {
            responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}
