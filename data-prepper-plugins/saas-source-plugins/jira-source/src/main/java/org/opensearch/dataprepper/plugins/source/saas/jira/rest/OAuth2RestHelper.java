package org.opensearch.dataprepper.plugins.source.saas.jira.rest;

import com.google.gson.JsonObject;
import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.JiraOauthConfig;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.AUTHORIZATION_ERROR_CODE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAuth2_URL;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.REST_API_FETCH_ISSUE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.TOKEN_EXPIRED;

public class OAuth2RestHelper {
    private static final Logger log = LoggerFactory.getLogger(OAuth2RestHelper.class);

    private static String getJiraAccountCloudId(JiraSourceConfig config) {
        log.info("Getting Jira Account Cloud ID");
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(config.getAccessToken());
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        int retryCount = 0;
        while(retryCount < RETRY_ATTEMPT) {
            retryCount++;
           try {
               ResponseEntity<Object> exchangeResponse =
                       restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
               List listResponse = (ArrayList)exchangeResponse.getBody();
               Map<String, Object> response = (Map<String, Object>) listResponse.get(0);
               return (String)response.get("id");
           } catch (HttpClientErrorException e) {
               if(e.getStatusCode().value() == TOKEN_EXPIRED) {
                   tryRefreshingAccessToken(config);
                   throw new UnAuthorizedException("Access token expired", e);
               }
               log.error("Error occurred while accessing resources: ", e);
               throw new UnAuthorizedException("Failed to access resources", e);
           }
       }
        return "";
    }

    public static void tryRefreshingAccessToken(JiraSourceConfig config) {
        log.info("Trying to refresh the access token");
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(config.getAccessToken());
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

        HttpEntity<Void> entity = new HttpEntity<>(headers);

        try {
            ResponseEntity<Object> exchangeResponse =
                    restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
            List listResponse = (ArrayList)exchangeResponse.getBody();
            Map<String, Object> response = (Map<String, Object>) listResponse.get(0);
            JiraOauthConfig oAuthConfig = JiraOauthConfig.getInstance(config);
            oAuthConfig.setAccessToken((String)response.get("id"));
        } catch (HttpClientErrorException e) {
            if(e.getStatusCode().value() == TOKEN_EXPIRED) {
                throw new UnAuthorizedException("Access token expired", e);

            }
            log.error("Error occurred while accessing resources: ", e);
            throw new UnAuthorizedException("Failed to access resources", e);
        }

    }


    private static HashMap<String, Object> createAccessRefreshTokenPair(
            String clientId, String clientSecret, String refreshToken) {
        log.info("Creating access-refresh token pair for Jira Connector.");
        RestTemplate restTemplate = new RestTemplate();
        HashMap<String, Object> oauthValues = new HashMap<>();
        try {
            String tokenEndPoint = Constants.TOKEN_LOCATION;

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            JsonObject obj = new JsonObject();
            obj.addProperty("grant_type", "refresh_token");
            obj.addProperty("client_id", clientId);
            obj.addProperty("client_secret", clientSecret);
            obj.addProperty("refresh_token", refreshToken);
            String payload = obj.toString();
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);

            ResponseEntity<Map> exchange = restTemplate.exchange(
                    tokenEndPoint,
                    HttpMethod.POST,
                    entity,
                    Map.class
            );
            Map<String, Object> oauthClientResponse = exchange.getBody();
            String newAccessToken = (String) oauthClientResponse.get(Constants.ACCESS_TOKEN);
            String newRefreshToken = (String) oauthClientResponse.get(Constants.REFRESH_TOKEN);

            if (StringUtils.hasLength(newAccessToken)) {
                log.debug("Access token is empty or null");
                throw new RuntimeException("Access token is empty or null");
            }
            if (StringUtils.hasLength(newRefreshToken)) {
                log.debug("Refresh token is empty or null ");
                throw new RuntimeException("Refresh token is empty or null");
            }
            oauthValues.put(Constants.ACCESS_TOKEN, newAccessToken);
            oauthValues.put(Constants.REFRESH_TOKEN, newRefreshToken);

        } catch (Exception e) {
            if (e.getMessage().contains(AUTHORIZATION_ERROR_CODE)) {
                log.error("An Authorization Exception exception has occurred while building"
                                + " request for request access token {} ", e.getMessage());
            }
        }
        return oauthValues;
    }

    /**
     * Method for getting Jira url based on auth type.
     * @return String
     */
    public static String getAuthTypeBasedJiraUrl(JiraSourceConfig jiraSourceConfig) {
        //For OAuth based flow, we use a different Jira url
        String authType = jiraSourceConfig.getAuthType();
        if(OAUTH2.equals(authType)){
            String cloudId = getJiraAccountCloudId(jiraSourceConfig);
            return OAuth2_URL + cloudId + "/" + REST_API_FETCH_ISSUE;
        }else {
            return jiraSourceConfig.getAccountUrl() + REST_API_FETCH_ISSUE + "/";
        }
    }
    
}
