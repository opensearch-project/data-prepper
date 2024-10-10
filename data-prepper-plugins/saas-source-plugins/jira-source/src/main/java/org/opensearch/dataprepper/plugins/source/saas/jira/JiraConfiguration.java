package org.opensearch.dataprepper.plugins.source.saas.jira;


import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.saas.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.ErrorCodeEnum;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.ExceptionUtil;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.MAX_CHARACTERS_LENGTH;

/**
 * The type Jira configuration.
 */
@Slf4j
@Value
@AllArgsConstructor(staticName = "of")
public class JiraConfiguration {
  /**
   * The constant JIRA_ACCOUNT_URL.
   */
  public static final String JIRA_ACCOUNT_URL = "jiraAccountUrl";
  /**
   * The constant JIRA_OAUTH_URL.
   */
  public static final String JIRA_OAUTH_URL = "jiraOauthUrl";
  /**
   * The constant ISSUE_STATUS_FILTER.
   */
  public static final String ISSUE_STATUS_FILTER = "status";
  /**
   * The constant ISSUE_TYPE_FILTER.
   */
  public static final String ISSUE_TYPE_FILTER = "issuetype";
  /**
   * The constant ISSUE_SUB_ENTITY_FILTER.
   */
  public static final String ISSUE_SUB_ENTITY_FILTER = "issueSubEntityFilter";
  /**
   * The constant PROJECT_KEY_FILTER.
   */
  public static final String PROJECT_KEY_FILTER = "project";
  /**
   * The constant CRAWL_TYPE.
   */
  public static final String CRAWL_TYPE = "crawlType";
  /**
   * The constant AUTH_TYPE.
   */
  public static final String AUTH_TYPE = "authType";
  /**
   * The constant JIRA_ID.
   */
  public static final String JIRA_ID = "jiraId";
  /**
   * The constant JIRA_CREDENTIAL.
   */
  public static final String JIRA_CREDENTIAL = "jiraCredential";
  /**
   * The constant JIRA_ACCESS_TOKEN.
   */
  public static final String JIRA_ACCESS_TOKEN = "jiraAccessToken";
  /**
   * The constant JIRA_REFRESH_TOKEN.
   */
  public static final String JIRA_REFRESH_TOKEN = "jiraRefreshToken";


  JiraSourceConfig repositoryConfiguration;

  /**
   * Fetch inclusion patterns from repository configuration.
   *
   * @return get inclusion patterns.
   */
  /*public List<String> getInclusionPatterns() {
    List<String> lst = this.repositoryConfiguration.getInclusionPatterns()!=null
            ? new ArrayList<>()
            : this.repositoryConfiguration.getInclusionPatterns();
    if (lst.size() > MAX_CHARACTERS_LENGTH) {
      log.error(String.valueOf(ErrorCodeEnum.ERROR_JIRA_INCLUSION_PATTERN),
              Constants.SOLUTION_FOR_INCLUSION_PATTERN);
      throw new InvalidPluginConfigurationException(
              ErrorCodeEnum.ERROR_JIRA_INCLUSION_PATTERN +
              Constants.SOLUTION_FOR_INCLUSION_PATTERN);
    } else {
      List<String> charLengthExceedingPatterns = lst.stream()
              .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
              .collect(Collectors.toList());
      if (!CollectionUtils.isNullOrEmpty(charLengthExceedingPatterns)) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.INCLUSION_PATTERN_OBJECT_VALUE),
                Constants.SOLUTION_FOR_INCLUSION_PATTERN_OBJECT_VALUE));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.INCLUSION_PATTERN_OBJECT_VALUE),
                Constants.SOLUTION_FOR_INCLUSION_PATTERN_OBJECT_VALUE));
      }
    }
    return lst;
  }*/

  /**
   * Fetch exclusion patterns from repository configuration.
   *
   * @return get exclusion patterns.
   */
 /* public List<String> getExclusionPatterns() {
    List<String> lst = CollectionUtils.isNullOrEmpty(
        this.repositoryConfiguration.getExclusionPatterns())
            ? new ArrayList<>()
            : this.repositoryConfiguration.getExclusionPatterns();
    if (lst.size() > 1000) {
      log.error(ExceptionUtil.getErrorMessage(
              String.valueOf(ErrorCodeEnum.ERROR_JIRA_EXCLUSION_PATTERN),
              Constants.SOLUTION_FOR_EXCLUSION_PATTERN));
      throw new BadRequestException(ExceptionUtil.getErrorMessage(
              String.valueOf(ErrorCodeEnum.ERROR_JIRA_EXCLUSION_PATTERN),
              Constants.SOLUTION_FOR_EXCLUSION_PATTERN));
    } else {
      List<String> charLengthExceedingPatterns = lst.stream()
              .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
              .collect(Collectors.toList());
      if (!CollectionUtils.isNullOrEmpty(charLengthExceedingPatterns)) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.EXCLUSION_PATTERN_OBJECT_VALUE),
                Constants.SOLUTION_FOR_EXCLUSION_PATTERN_OBJECT_VALUE));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.EXCLUSION_PATTERN_OBJECT_VALUE),
                Constants.SOLUTION_FOR_EXCLUSION_PATTERN_OBJECT_VALUE));
      }
    }
    return lst;
  }*/



/*
  */
/**
   * Get Issue Sub Entities Filter from repository configuration.
   *
   * @return List Sub Entities Filter.
   *//*

  public List<String> getIssueSubEntityFilter() {
    List<String> lst = (List<String>)
            this.repositoryConfiguration.getAdditionalProperties()
                .get(ISSUE_SUB_ENTITY_FILTER);
    if (!CollectionUtils.isNullOrEmpty(lst)) {
      if (lst.size() > 1000) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_SUB_ENTITY_FILTER_PATTERN),
                Constants.SOLUTION_FOR_ISSUE_SUB_ENTITY_FILTER));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_SUB_ENTITY_FILTER_PATTERN),
                Constants.SOLUTION_FOR_ISSUE_SUB_ENTITY_FILTER));
      } else {
        List<String> charLengthExceedingPatterns = lst.stream()
                .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
                .collect(Collectors.toList());
        if (!CollectionUtils.isNullOrEmpty(charLengthExceedingPatterns)) {
          log.error(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_ISSUE_SUB_ENTITY_FILTER_VALUE),
                  Constants.SOLUTION_FOR_ISSUE_SUB_ENTITY_FILTER_OBJECT_VALUE));
          throw new BadRequestException(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_ISSUE_SUB_ENTITY_FILTER_VALUE),
                  Constants.SOLUTION_FOR_ISSUE_SUB_ENTITY_FILTER_OBJECT_VALUE));
        }
      }
    }
    return lst;
  }
*/

  /**
   * Get Issue Status Filter from repository configuration.
   *
   * @return List Issue Status Filter.
   */
  public List<String> getIssueStatusFilter() {
    List<String> issueStatusFilter = (List<String>)
            this.repositoryConfiguration.getAdditionalProperties().get(ISSUE_STATUS_FILTER);
    if (!CollectionUtils.isEmpty(issueStatusFilter)) {
      if (issueStatusFilter.size() > 1000) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_STATUS_FILTER_PATTERN),
                Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_STATUS_FILTER_PATTERN),
                Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER));
      } else {
        List<String> charLengthExceedingPatterns = issueStatusFilter.stream()
                .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(charLengthExceedingPatterns)) {
          log.error(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_STATUS_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER_OBJECT_VALUE));
          throw new BadRequestException(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_STATUS_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_ISSUE_STATUS_FILTER_OBJECT_VALUE));
        }
      }
    }
    return issueStatusFilter;
  }

  /**
   * Get Issue Types Filter from repository configuration.
   *
   * @return List Issue Type Filter.
   */
  public List<String> getIssueTypeFilter() {
    List<String> issueTypeFilter = (List<String>)
            this.repositoryConfiguration.getAdditionalProperties().get(ISSUE_TYPE_FILTER);
    if (!CollectionUtils.isEmpty(issueTypeFilter)) {
      if (issueTypeFilter.size() > 1000) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_TYPE_FILTER),
                Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_ISSUE_TYPE_FILTER),
                Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER));
      } else {
        List<String> charLengthExceedingPatterns = issueTypeFilter.stream()
                .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(charLengthExceedingPatterns)) {
          log.error(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_TYPE_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER_OBJECT_VALUE));
          throw new BadRequestException(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_ISSUE_TYPE_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_ISSUE_TYPE_FILTER_OBJECT_VALUE));
        }
      }
    }
    return issueTypeFilter;
  }

  /**
   * Get Project Filter Types from repository configuratio
   * public static final String ST = "status";n.
   *
   * @return List Project Filter.
   */
  public List<String> getProjectKeyFilter() {
    List<String> projectKeyFilter = this.repositoryConfiguration.getProject();
    if (!CollectionUtils.isEmpty(projectKeyFilter)) {
      if (projectKeyFilter.size() > 1000) {
        log.error(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_PROJECT_KEY_FILTER),
                Constants.SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER));
        throw new BadRequestException(ExceptionUtil.getErrorMessage(
                String.valueOf(ErrorCodeEnum.ERROR_JIRA_PROJECT_KEY_FILTER),
                Constants.SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER));
      } else {
        List<String> charLengthExceedingPatterns = projectKeyFilter.stream()
                .filter(pattern -> pattern.length() > MAX_CHARACTERS_LENGTH)
                .collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(charLengthExceedingPatterns)) {
          log.error(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_PROJECT_KEY_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER_OBJECT_VALUE));
          throw new BadRequestException(ExceptionUtil.getErrorMessage(
                  String.valueOf(ErrorCodeEnum.JIRA_PROJECT_KEY_FILTER_VALUE),
                  Constants.SOLUTION_FOR_JIRA_PROJECT_KEY_FILTER_OBJECT_VALUE));
        }
      }
    }
    return projectKeyFilter;
  }

  /**
   * Fetch Jira Account Url from repository configuration.
   *
   * @return get url.
   */
  public String getJiraAccountUrl() {
    return this.repositoryConfiguration.getAccountUrl();
  }

  /**
   * Fetch Jira Oauth Url from repository configuration.
   *
   * @return get url.
   */
  public String getJiraOauthUrl() {
    return this.repositoryConfiguration.getAccountUrl();
  }

  /**
   * Fetch Auth type from repository configuration.
   *
   * @return get token.
   */
  public String getAuthType() {
    return "Basic";
  }

  /**
   * Fetch Jira Id from repository configuration.
   *
   * @return get token.
   */
  public String getJiraId() {
    return (String) this.repositoryConfiguration.getConnectorCredentials().get("jira_id");
  }

  /**
   * Fetch Jira Credentials from repository configuration.
   *
   * @return get token.
   */
  public String getJiraCredential() {
    return (String) this.repositoryConfiguration.getConnectorCredentials().get("jira_credential");
  }


  /**
   * Fetch Jira Access Token from repository configuration.
   *
   * @return get token.
   */
  public String getJiraAccessToken() {
    return (String) this.repositoryConfiguration.getConnectorCredentials().get("jiraAccessToken");
  }

  /**
   * Fetch Jira Refresh Token from repository configuration.
   *
   * @return get token.
   */
  public String getJiraRefreshToken() {
    return (String) this.repositoryConfiguration.getConnectorCredentials().get("jiraRefreshToken");
  }


}
