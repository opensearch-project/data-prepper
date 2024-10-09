package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class BasicAuthRestTemplateConfig {

    @Bean
    public RestTemplate basicAuthRestTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        restTemplate.getInterceptors().add(new BasicAuthInterceptor("your-username", "your-password"));
        return restTemplate;
    }
}
