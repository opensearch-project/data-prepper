package org.opensearch.dataprepper.plugins.source.jira.rest.auth;

import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.slf4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.JqlConstants.SLASH;

/**
 * The type Jira service.
 */

public class JiraOauthConfig implements JiraAuthConfig {

    public static final String OAuth2_URL = "https://api.atlassian.com/ex/jira/";
    public static final String ACCESSIBLE_RESOURCES = "https://api.atlassian.com/oauth/token/accessible-resources";
    public static final String TOKEN_LOCATION = "https://auth.atlassian.com/oauth/token";

    public static final String EXPIRES_IN = "expires_in";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String ACCESS_TOKEN = "access_token";
    private static final Logger log =
            org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);
    private final String clientId;
    private final String clientSecret;
    private final JiraSourceConfig jiraSourceConfig;
    private final Object cloudIdFetchLock = new Object();
    private final Object tokenRenewLock = new Object();
    RestTemplate restTemplate = new RestTemplate();
    private String url;
    @Getter
    private int expiresInSeconds = 0;
    @Getter
    private Instant expireTime;
    @Getter
    private String accessToken;
    @Getter
    private String refreshToken;
    @Getter
    private String cloudId = null;

    public JiraOauthConfig(JiraSourceConfig jiraSourceConfig) {
        this.jiraSourceConfig = jiraSourceConfig;
        this.accessToken = jiraSourceConfig.getAccessToken();
        this.refreshToken = jiraSourceConfig.getRefreshToken();
        this.clientId = jiraSourceConfig.getClientId();
        this.clientSecret = jiraSourceConfig.getClientSecret();
    }

    private String getJiraAccountCloudId() {
        log.info("Getting Jira Account Cloud ID");
        synchronized (cloudIdFetchLock) {
            if (this.cloudId != null) {
                //Someone else must have initialized it
                return this.cloudId;
            }

            int retryCount = 0;
            while (retryCount < RETRY_ATTEMPT) {
                retryCount++;
                try {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setBearerAuth(accessToken);
                    headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
                    HttpEntity<Void> entity = new HttpEntity<>(headers);
                    ResponseEntity<Object> exchangeResponse =
                            restTemplate.exchange(ACCESSIBLE_RESOURCES, HttpMethod.GET, entity, Object.class);
                    List<Map<String, Object>> listResponse = (List<Map<String, Object>>) exchangeResponse.getBody();
                    Map<String, Object> response = listResponse.get(0);
                    return (String) response.get("id");
                } catch (HttpClientErrorException e) {
                    if (e.getRawStatusCode() == HttpStatus.UNAUTHORIZED.value()) {
                        renewCredentials();
                    }
                    log.error("Error occurred while accessing resources: ", e);
                }
            }
            throw new UnAuthorizedException(String.format("Access token expired. Unable to renew even after %s attempts", RETRY_ATTEMPT));
        }
    }

    public void renewCredentials() {
        Instant currentTime = Instant.now();
        if (expireTime != null && expireTime.isAfter(currentTime)) {
            //There is still time to renew or someone else must have already renewed it
            return;
        }

        synchronized (tokenRenewLock) {
            if (expireTime != null && expireTime.isAfter(currentTime)) {
                //Someone else must have already renewed it
                return;
            }

            log.info("Renewing access-refresh token pair for Jira Connector.");
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            String payloadTemplate = "{\"grant_type\": \"%s\", \"client_id\": \"%s\", \"client_secret\": \"%s\", \"refresh_token\": \"%s\"}";
            String payload = String.format(payloadTemplate, "refresh_token", clientId, clientSecret, refreshToken);
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);

            try {
                ResponseEntity<Map> responseEntity = restTemplate.postForEntity(TOKEN_LOCATION, entity, Map.class);
                Map<String, Object> oauthClientResponse = responseEntity.getBody();
                this.accessToken = (String) oauthClientResponse.get(ACCESS_TOKEN);
                this.refreshToken = (String) oauthClientResponse.get(REFRESH_TOKEN);
                this.expiresInSeconds = (int) oauthClientResponse.get(EXPIRES_IN);
                this.expireTime = Instant.ofEpochMilli(System.currentTimeMillis() + (expiresInSeconds * 1000L));
            } catch (HttpClientErrorException ex) {
                this.expireTime = null;
                this.expiresInSeconds = 0;
                log.error("Failed to renew access token. Status code: {}, Error Message: {}",
                        ex.getRawStatusCode(), ex.getMessage());
                throw new RuntimeException("Failed to renew access token" + ex.getMessage(), ex);
            }
        }
    }

    @Override
    public String getUrl() {
        if (url == null || url.isEmpty()) {
            synchronized (cloudIdFetchLock) {
                if (url == null || url.isEmpty()) {
                    initCredentials();
                }
            }
        }
        return url;
    }

    /**
     * Method for getting Jira url based on auth type.
     */
    @Override
    public void initCredentials() {
        //For OAuth based flow, we use a different Jira url
        this.cloudId = getJiraAccountCloudId();
        this.url = OAuth2_URL + this.cloudId + SLASH;
    }
}
