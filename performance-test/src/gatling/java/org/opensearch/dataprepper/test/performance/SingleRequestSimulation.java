/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.test.performance;

import io.gatling.javaapi.core.ChainBuilder;
import io.gatling.javaapi.core.CoreDsl;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpDsl;
import org.opensearch.dataprepper.test.performance.tools.PathTarget;
import org.opensearch.dataprepper.test.performance.tools.Protocol;

public class SingleRequestSimulation extends Simulation {
    ChainBuilder sendSingleLogFile = CoreDsl.exec(
            HttpDsl.http("Post log")
                    .post(PathTarget.getPath())
                    .body(CoreDsl.ElFileBody("bodies/singleLog.json"))
                    .asJson()
                    .check(HttpDsl.status().is(200), CoreDsl.responseTimeInMillis().lt(200))
    );

    ScenarioBuilder basicScenario = CoreDsl.scenario("Post static json log file")
            .exec(sendSingleLogFile);


    {

        setUp(
                basicScenario.injectOpen(CoreDsl.atOnceUsers(1))
        ).protocols(
                Protocol.httpProtocol()
        ).assertions(
                CoreDsl.global().responseTime().mean().lt(100),
                CoreDsl.global().successfulRequests().percent().is(100.0)
        );
    }
}
