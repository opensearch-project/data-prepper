package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ACCESSIBLE_RESOURCES;


public class OAuth2RestHelper {
    private static Logger log = LoggerFactory.getLogger(OAuth2RestHelper.class);

    public static String getJiraAccountCloudId(JiraSourceConfig config) {
        log.info("Getting Jira Account Cloud ID");
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(config.getAccessToken());
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        RestTemplate  restTemplate = new RestTemplate();
        ResponseEntity<Object> exchangeResponse = restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
        List listResponse = (ArrayList)exchangeResponse.getBody();
        Map<String, Object> response = (Map<String, Object>) listResponse.get(0);
        return (String)response.get("id");
    }
}
