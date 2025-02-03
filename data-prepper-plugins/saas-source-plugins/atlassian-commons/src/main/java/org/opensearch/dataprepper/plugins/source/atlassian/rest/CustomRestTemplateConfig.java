/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.atlassian.rest;


import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AtlassianSourceConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import static org.opensearch.dataprepper.plugins.source.atlassian.utils.Constants.OAUTH2;

@Configuration
public class CustomRestTemplateConfig {

    @Bean
    public RestTemplate basicAuthRestTemplate(AtlassianSourceConfig config, AtlassianAuthConfig authConfig) {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        ClientHttpRequestInterceptor httpInterceptor;
        if (OAUTH2.equals(config.getAuthType())) {
            httpInterceptor = new OAuth2RequestInterceptor(authConfig);
        } else {
            httpInterceptor = new BasicAuthInterceptor(config);
        }
        restTemplate.getInterceptors().add(httpInterceptor);
        return restTemplate;
    }


}
