/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.meter;

import io.micrometer.core.util.internal.logging.InternalLogger;
import io.micrometer.core.util.internal.logging.InternalLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.config.Configuration;
import software.amazon.cloudwatchlogs.emf.config.EnvironmentConfigurationProvider;
import software.amazon.cloudwatchlogs.emf.environment.DefaultEnvironment;
import software.amazon.cloudwatchlogs.emf.environment.Environment;
import software.amazon.cloudwatchlogs.emf.environment.EnvironmentProvider;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import java.net.URISyntaxException;

public class TestLogger {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(TestLogger.class);
    private static final Logger logger = LoggerFactory.getLogger(TestLogger.class);

    public static void main(String[] args) throws URISyntaxException {
        Configuration configuration = EnvironmentConfigurationProvider.getConfig();
        configuration.setAgentEndpoint("localhost");
        EnvironmentProvider environmentProvider = new EnvironmentProvider();
        Environment environment = new DefaultEnvironment(configuration);
        MetricsLogger metrics = new MetricsLogger(environment);
        DimensionSet dimensionSet = new DimensionSet();
        dimensionSet.addDimension("Service", "Aggregator");
        metrics.setDimensions(dimensionSet);
        metrics.putMetric("ProcessingLatency", 100, Unit.MILLISECONDS);
//        metrics.putProperty("RequestId", "422b1569-16f6-4a03-b8f0-fe3fd9b100f8");
        metrics.flush();
    }
}
