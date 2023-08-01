/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.prometheus.configuration.PrometheusSinkConfiguration;

/**
 * This service class contains logic for handling AWS related services
 */
public class PrometheusSinkAwsService {

    public static void  attachSigV4(final PrometheusSinkConfiguration prometheusSinkConfiguration, final HttpClientBuilder httpClientBuilder, final AwsCredentialsSupplier awsCredentialsSupplier) {
     // TODO: implementation
    }
}
