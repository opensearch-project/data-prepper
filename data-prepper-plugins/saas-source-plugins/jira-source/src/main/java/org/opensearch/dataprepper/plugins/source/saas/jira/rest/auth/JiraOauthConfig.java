package org.opensearch.dataprepper.plugins.source.saas.jira.rest.auth;

import com.google.gson.JsonObject;
import lombok.Getter;
import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants;
import org.slf4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.AUTHORIZATION_ERROR_CODE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAuth2_URL;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.SLASH;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.TOKEN_EXPIRED;

/**
 * The type Jira service.
 */

@Getter
public class JiraOauthConfig implements JiraAuthConfig {

  private static final Logger log =
          org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);

  private final String clientId;
  private final String clientSecret;
  private String accessToken;
  private String refreshToken;
  private String url;
  private String cloudId;

  private final JiraSourceConfig jiraSourceConfig;

  public JiraOauthConfig(JiraSourceConfig jiraSourceConfig) {
    this.jiraSourceConfig = jiraSourceConfig;
    this.accessToken = jiraSourceConfig.getAccessToken();
    this.refreshToken = jiraSourceConfig.getRefreshToken();
    this.clientId = jiraSourceConfig.getClientId();
    this.clientSecret = jiraSourceConfig.getClientSecret();
  }

  private synchronized String getJiraAccountCloudId(JiraSourceConfig config) {
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
          renewCredentials();
        }
        log.error("Error occurred while accessing resources: ", e);
      }
    }
    throw new UnAuthorizedException(String.format("Access token expired. Unable to renew even after %s attempts", RETRY_ATTEMPT));
  }

  public synchronized void renewCredentials() {
    log.info("Renewing access-refresh token pair for Jira Connector.");
    RestTemplate restTemplate = new RestTemplate();
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

      if (!StringUtils.hasLength(newAccessToken)) {
        log.debug("Access token is empty or null");
        throw new RuntimeException("Access token is empty or null");
      }
      if (!StringUtils.hasLength(newRefreshToken)) {
        log.debug("Refresh token is empty or null ");
        throw new RuntimeException("Refresh token is empty or null");
      }

      this.accessToken = newAccessToken;
      this.refreshToken = newRefreshToken;

    } catch (Exception e) {
      if (e.getMessage().contains(AUTHORIZATION_ERROR_CODE)) {
        log.error("An Authorization Exception exception has occurred while renewing access token {} ", e.getMessage());
      }
    }
  }

  @Override
  public String getUrl() {
    if(url==null || url.isEmpty()) {
      synchronized (this) {
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
      this.cloudId = getJiraAccountCloudId(jiraSourceConfig);
      this.url = OAuth2_URL + this.cloudId + SLASH;
  }
}
