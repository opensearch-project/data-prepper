/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.opensearch.dataprepper.test.performance.tools.Protocol;
import org.opensearch.dataprepper.test.performance.tools.TraceChain;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class SlowBurnTraceSimulation extends Simulation {
    private static final Integer batchSize = Integer.getInteger("batchSize", 200);
    private static final Integer rampUsers = Integer.getInteger("rampUsers", 60);
    private static final Duration rampUpTime = Duration.ofMinutes(Integer.getInteger("rampUpTime", 60));
    private static final Duration peakDuration = Duration.ofMinutes(Integer.getInteger("rampUpTime", 5));
    private static final List<String> endpoints = Arrays.asList(System.getProperty("endpoints").split(","));

    ScenarioBuilder rampUpScenario = CoreDsl.scenario("Slow Burn")
            .forever()
            .on(TraceChain.sendUnframedExportTraceServiceRequest(endpoints, batchSize));

    {
        setUp(
                rampUpScenario.injectOpen(
                        CoreDsl.rampUsers(rampUsers).during(rampUpTime),
                        CoreDsl.nothingFor(peakDuration)
                )
        )
                .maxDuration(rampUpTime.plus(peakDuration))
                .protocols(Protocol.httpProtocol());
    }
}
