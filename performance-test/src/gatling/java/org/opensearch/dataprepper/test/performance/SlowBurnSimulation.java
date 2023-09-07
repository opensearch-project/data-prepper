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

public class SlowBurnSimulation extends Simulation {
    private static final Integer largeBatchSize = 200;
    private static final Integer rampUsers = 60;
    private static final Duration rampUpTime = Duration.ofMinutes(60);
    private static final Duration peakDuration = Duration.ofMinutes(5);

    ScenarioBuilder rampUpScenario = CoreDsl.scenario("Slow Burn")
            .forever()
            .on(Chain.sendApacheCommonLogPostRequest("Post logs with large batch", largeBatchSize));

    public SlowBurnSimulation()
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
