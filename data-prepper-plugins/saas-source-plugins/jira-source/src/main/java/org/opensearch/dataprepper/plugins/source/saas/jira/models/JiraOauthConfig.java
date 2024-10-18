package org.opensearch.dataprepper.plugins.source.saas.jira.models;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.dataprepper.plugins.source.saas.jira.JiraSourceConfig;
import org.slf4j.Logger;

/**
 * The type Jira service.
 */

@Setter
@Getter
public class JiraOauthConfig {

  private static final Logger appLog =
          org.slf4j.LoggerFactory.getLogger(JiraOauthConfig.class);

  private static volatile JiraOauthConfig instance;
  private String accessToken;
  private String refreshToken = "";
  private String clientId = "";
  private String clientSecret = "";
  private String url;

  private JiraOauthConfig(JiraSourceConfig config) {
    this.accessToken = config.getAccessToken();
    this.accessToken = config.getRefreshToken();
    this.accessToken = config.getClientId();
    this.accessToken = config.getClientSecret();
  }

  public static JiraOauthConfig getInstance(JiraSourceConfig config) {
    if (instance == null) {
      synchronized (JiraOauthConfig.class) {
        if (instance == null) {
          instance = new JiraOauthConfig(config);
        }
      }
    }
    return instance;
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

  */
}
