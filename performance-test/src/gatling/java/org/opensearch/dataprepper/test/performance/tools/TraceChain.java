/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance.tools;

import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.Session;
import io.gatling.javaapi.http.HttpDsl;

import java.util.List;
import java.util.function.Function;

public class TraceChain {

    public static ChainBuilder sendUnframedExportTraceServiceRequest(final List<String> endpoints,
                                                                     final int batchSize) {
        ChainBuilder chainBuilder = ChainBuilder.EMPTY;
        for (final String endpoint : endpoints) {
            final Function<Session, String> requestTemplate = TraceTemplates.exportTraceServiceRequestTemplate(batchSize);
            chainBuilder = chainBuilder.exec(
                    HttpDsl.http("Post to " + endpoint)
                            .post("http://" + endpoint + "/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                            .body(CoreDsl.StringBody(requestTemplate)));
        }
        return chainBuilder;
    }
}
