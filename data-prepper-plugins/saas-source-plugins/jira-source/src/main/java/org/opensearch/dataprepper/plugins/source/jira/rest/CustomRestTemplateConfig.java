package org.opensearch.dataprepper.plugins.source.jira.rest;


import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;

@Configuration
public class CustomRestTemplateConfig {

    @Bean
    public RestTemplate basicAuthRestTemplate(JiraSourceConfig config, JiraAuthConfig authConfig) {
        RestTemplate restTemplate = new RestTemplateRetryable(3);
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
