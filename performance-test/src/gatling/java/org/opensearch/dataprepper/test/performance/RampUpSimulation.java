/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import org.opensearch.dataprepper.test.performance.tools.Chain;
import org.opensearch.dataprepper.test.performance.tools.Protocol;

import java.time.Duration;

public class RampUpSimulation extends Simulation {
    private static final Integer largeBatchSize = 200;
    private static final Integer rampUsers = 40;
    private static final Duration rampUpTime = Duration.ofSeconds(30);
    private static final Duration peakLoadTime = Duration.ofMinutes(10);

    ScenarioBuilder rampUpScenario = CoreDsl.scenario("Ramp Up Scenario")
            .during(peakLoadTime)
            .on(Chain.sendApacheCommonLogPostRequest("Post logs with large batch", largeBatchSize));

    public RampUpSimulation()
    {
        setUp(
                rampUpScenario.injectOpen(
                        CoreDsl.rampUsers(rampUsers).during(rampUpTime)
                )
        ).protocols(
                Protocol.httpProtocol()
        ).assertions(
                CoreDsl.global().failedRequests().percent().lt(1.0),
                CoreDsl.global().responseTime().mean().lt(600),
                CoreDsl.global().responseTime().max().lt(10000),
                CoreDsl.global().requestsPerSec().gt(100.0)
        );
    }
}
