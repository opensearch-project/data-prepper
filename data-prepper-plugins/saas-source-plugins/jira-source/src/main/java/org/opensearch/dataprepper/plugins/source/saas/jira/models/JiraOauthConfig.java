package org.opensearch.dataprepper.plugins.source.saas.jira.models;


import org.slf4j.Logger;

/**
 * The type Jira service.
 */
public class JiraOauthConfig {
  /**
   * The Access token.
   */
  static String accessToken = "";
  /**
   * The Refresh token.
   */
  static String refreshToken = "";
  /**
   * The Client id.
   */
  static String clientId = "";
  /**
   * The Client secret.
   */
  static String clientSecret = "";
  private static final Logger log =
          org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);

  /**
   * Set OauthConfigValues initially.
   */
  static synchronized void setOauthConfigValues(String jiraId,
                                                String jiraCredential, String jiraAccessToken,
                                                String jiraRefreshToken) {
    log.info("Setting OAuth configuration values for Jira Connector.");

    if (!jiraId.equals(clientId)) {
      clientId = jiraId;
    }
    if (!jiraCredential.equals(clientSecret)) {
      clientSecret = jiraCredential;
    }
    if (!jiraAccessToken.equals(accessToken)) {
      accessToken = jiraAccessToken;
    }
    if (!jiraRefreshToken.equals(refreshToken)) {
      refreshToken = jiraRefreshToken;
    }
  }

  /**
   * Get changed Access and Refresh Token when old expired.
   */
  /*static synchronized void changeAccessAndRefreshToken(Object jiraConfiguration) {
    log.info("Setting access-refresh token for Jira Connector.");
    boolean configuaration = true;

    if (!configuaration) {
      Map<String, Object> oauthValues = createAccessRefreshTokenPair(
              clientId, clientSecret, refreshToken);
      String accessTokenNew = oauthValues.get(Constants.ACCESS_TOKEN).toString();
      String refreshTokenNew = oauthValues.get(Constants.REFRESH_TOKEN).toString();
      if (accessTokenNew!=null && !refreshTokenNew.isEmpty()) {
        accessToken = accessTokenNew;
        refreshToken = refreshTokenNew;
      }
    }
  }*/

  /**
   * create AccessRefreshToken Pair.
   *
   * @param clientId the configuration
   * @return AccessRefreshToken
   */
  /*private static HashMap<String, Object> createAccessRefreshTokenPair(
          String clientId, String clientSecret, String refreshToken) {
    log.info("Creating access-refresh token pair for Jira Connector.");
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
    return oauthValues;
  }*/
}