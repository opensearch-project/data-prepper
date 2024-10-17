package org.opensearch.dataprepper.plugins.source.saas.jira;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants;
import org.slf4j.Logger;
import org.springframework.util.StringUtils;

import javax.inject.Named;
import java.util.HashMap;
import java.util.Map;

/**
 * The type Jira service.
 */

@Setter
@Getter
public class JiraOauthConfig {

  private static final Logger appLog =
          org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);


  private String accessToken = "";
  private String refreshToken = "";
  private String clientId = "";
  private String clientSecret = "";

  public JiraOauthConfig(String accessToken, String refreshToken, String clientId, String clientSecret) {
    this.accessToken = accessToken;
    this.refreshToken = refreshToken;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
  }

  public JiraOauthConfig(JiraOauthConfig oauthConfig){
    this.accessToken = oauthConfig.getAccessToken();
    this.refreshToken = oauthConfig.getRefreshToken();
    this.clientId = oauthConfig.getClientId();
    this.clientSecret = oauthConfig.getClientSecret();
  }


   /* *//**
   * Get changed Access and Refresh Token when old expired.
   *//*
  static synchronized void changeAccessAndRefreshToken(Object jiraConfiguration) {
    appLog.info("Setting access-refresh token for Jira Connector.");
    boolean configuaration = true;
    if (jiraConfiguration instanceof JiraConfigHelper) {
      configuaration = JiraService. .reTestConnection((JiraConfigHelper) jiraConfiguration);
    }

    if (!configuaration) {
      HashMap<String, Object> oauthValues = createAccessRefreshTokenPair(
              clientId, clientSecret, refreshToken);
      String accessTokenNew = oauthValues.get(Constants.ACCESS_TOKEN).toString();
      String refreshTokenNew = oauthValues.get(Constants.REFRESH_TOKEN).toString();
      if (StringUtils.hasLength(accessTokenNew)
              && StringUtils.hasLength(refreshTokenNew)) {
        accessToken = accessTokenNew;
        refreshToken = refreshTokenNew;
      }
    }
  }

  *//**
   * create AccessRefreshToken Pair.
   *
   * @param clientId the configuration
   * @return AccessRefreshToken
   *//*
  private static HashMap<String, Object> createAccessRefreshTokenPair(
          String clientId, String clientSecret, String refreshToken) {
    *//*appLog.info("Creating access-refresh token pair for Jira Connector.");
    OAuthClientRequest accessRequest = null;
    HashMap<String, Object> oauthValues = new HashMap<>();
    try {
      String tokenEndPoint = Constants.TOKEN_LOCATION;
      accessRequest = OAuthClientRequest.tokenLocation(tokenEndPoint)
              .setGrantType(GrantType.REFRESH_TOKEN).setClientId(clientId)
              .setClientSecret(clientSecret).setRefreshToken(refreshToken)
              .buildBodyMessage();
      OAuthClient oauthClient = new OAuthClient(new URLConnectionClient());
      OAuthClientResponse oauthClientResponse = oauthClient.accessToken(accessRequest);
      String newAccessToken = oauthClientResponse.getParam(Constants.ACCESS_TOKEN);
      String newRefreshToken = oauthClientResponse.getParam(Constants.REFRESH_TOKEN);

      if (StringUtils.isBlank(newAccessToken)) {
        appLog.debug("Access token is empty or null");
        throw new RuntimeException("Access token is empty or null");
      }
      if (StringUtils.isBlank(newRefreshToken)) {
        appLog.debug("Refresh token is empty or null ");
        throw new RuntimeException("Refresh token is empty or null");
      }
      oauthValues.put(Constants.ACCESS_TOKEN, newAccessToken);
      oauthValues.put(Constants.REFRESH_TOKEN, newRefreshToken);

    } catch (OAuthProblemException | OAuthSystemException e) {
      if (e.getMessage().contains(AUTHORIZATION_ERROR_CODE)) {
        appLog.error("[{}] - An Authorization Exception exception has occurred while building"
                        + " request for request access token {} ", StackTraceLogger.getUuid(),
                e.getMessage());
        throw new ContinuableBadRequestException(e.getMessage(), e);
      }
    }
    return oauthValues;*//*
    return null;
  }*/
}
