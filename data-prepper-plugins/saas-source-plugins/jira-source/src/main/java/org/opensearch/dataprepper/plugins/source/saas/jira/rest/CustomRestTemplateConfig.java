package org.opensearch.dataprepper.plugins.source.saas.jira.rest;


import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAUTH2;

@Configuration
public class CustomRestTemplateConfig {

    @Bean
    public RestTemplate basicAuthRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        JiraSourceConfig config = new JiraSourceConfig();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        ClientHttpRequestInterceptor httpInterceptor;
        if(OAUTH2.equals(config.getAuthType())) {
            httpInterceptor = new OAuth2RequestInterceptor(config);
        }else {
            httpInterceptor = new BasicAuthInterceptor(config);
        }
        restTemplate.getInterceptors().add(httpInterceptor);
        return restTemplate;
    }

}
