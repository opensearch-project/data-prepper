/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import com.google.protobuf.util.JsonFormat;
import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.http.HttpDsl;

import java.util.List;

public class TraceChain {
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

    public static ChainBuilder sendUnframedExportTraceServiceRequest(final List<String> endpoints,
                                                                     final int batchSize) {
        ChainBuilder chainBuilder = ChainBuilder.EMPTY;
        chainBuilder = chainBuilder.exec(session -> {
            final List<String> requestJsons = TraceTemplates.exportTraceServiceRequestJsons(endpoints.size(), batchSize);
            for (int i = 0; i < endpoints.size(); i++) {
                session = session.set(endpoints.get(i), requestJsons.get(i));
            }
            return session;
        });
        for (final String endpoint : endpoints) {
            chainBuilder = chainBuilder.exec(
                    HttpDsl.http("Post to " + endpoint)
                            .post("http://" + endpoint + "/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                            .body(CoreDsl.StringBody(session -> session.getString(endpoint))));
        }
        return chainBuilder;
    }
}
