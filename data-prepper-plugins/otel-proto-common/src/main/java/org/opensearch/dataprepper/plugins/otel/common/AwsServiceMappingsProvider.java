/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.otel.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AwsServiceMappingsProvider implements ServiceMappingsProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AwsServiceMappingsProvider.class);
    private static final String SERVICE_MAPPINGS_FILE = "aws_service_mappings";
    private Map<String, String> serviceMappings;

    public AwsServiceMappingsProvider() {
        try (
            final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(SERVICE_MAPPINGS_FILE);
        ) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                serviceMappings = reader.lines()
                .filter(line -> line.contains(","))
                .collect(Collectors.toMap(
                    line -> line.split(",", 2)[0].trim(),
                    line -> line.split(",", 2)[1].trim(),
                    (oldValue, newValue) -> newValue
                ));
            } catch (IOException e) {
                throw e;
            }
        } catch (final Exception e) {
            LOG.error("An exception occurred while initializing service mappings for Data Prepper", e);
            serviceMappings = null;
        }
    }

    public Map<String, String> getServiceMappings() {
        return serviceMappings;
    }

}
