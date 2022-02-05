/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpDsl;
import org.opensearch.dataprepper.test.performance.tools.TraceChain;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class FixedClientTraceSimulation extends Simulation {
    private static final Integer batchSize = Integer.getInteger("batchSize", 20);
    private static final Integer users = Integer.getInteger("users", 10);
    private static final Duration duration = Duration.ofMinutes(Long.getLong("duration", 5));
    private static final List<String> endpoints = Arrays.asList(System.getProperty("endpoints").split(","));

    ScenarioBuilder fixedScenario = CoreDsl.scenario("Slow Burn")
            .during(duration)
            .on(TraceChain.sendUnframedExportTraceServiceRequest(endpoints, batchSize));

    {
        setUp(fixedScenario.injectOpen(CoreDsl.atOnceUsers(users)))
                .protocols(HttpDsl.http
                        .acceptHeader("application/json")
                        .header("Content-Type", "application/json; charset=utf-8"))
                .assertions(CoreDsl.global().requestsPerSec().gt(140.0));
    }
}
