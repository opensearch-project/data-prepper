package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.http.HttpHeaders;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.saas.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.saas.jira.rest.CustomRestTemplateConfig;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.AddressValidation;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants;
import org.opensearch.dataprepper.plugins.source.saas.jira.utils.JiraContentType;
import org.slf4j.Logger;
import org.springframework.security.oauth2.client.ClientAuthorizationRequiredException;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ACCEPT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.Application_JSON;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BAD_REQUEST_EXCEPTION;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BAD_RESPONSE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BASIC;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CONTENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.EMPTY_STRING;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ERR_MSG;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.EXPAND_VALUE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.FIFTY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_TYPE_ID;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.JQL_FIELD;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.LIVE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.MAX_RESULT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.MAX_RESULTS;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_IN;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_KEY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_NAME;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.REST_API_FETCH_ISSUE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.REST_API_SEARCH;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.START_AT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.STATUS_IN;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.SUCCESS_RESPONSE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.SUFFIX;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.TOKEN_EXPIRED;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants._ISSUE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants._PROJECT;


/**
 * The type Jira service.
 */

@Named
public class JiraService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(JiraService.class);
  private static final List<Integer> waitTimeList =
          Arrays.asList(1, 3, 5, 10, 20, 40, 60, 120, 240);

  private final RestTemplate restTemplate;

  private final JiraConfigHelper configHelper;

  private final CustomRestTemplateConfig customRestTemplateConfig;
  /**
   * The Jira project cache.
   */
  static Map<String, String> jiraProjectCache = new ConcurrentHashMap<>();

  public JiraService(RestTemplate restTemplate, JiraConfigHelper configHelper, CustomRestTemplateConfig customRestTemplateConfig) {
    this.restTemplate = restTemplate;
    this.configHelper = configHelper;
    this.customRestTemplateConfig = customRestTemplateConfig;
  }

  /**
   * Get jira entities.
   *
   * @param configuration the configuration.
   * @param timestamp timestamp.
   * @param itemInfoQueue Item info queue.
   * @param futureList Future list.
   * @param crawlerTaskExecutor Executor service.
   */
  public void getJiraEntities(JiraSourceConfig configuration, long timestamp,
                              Queue<ItemInfo> itemInfoQueue, List<Future<Boolean>> futureList,
                              ExecutorService crawlerTaskExecutor) {
    log.info("Started to fetch entities");
    jiraProjectCache.clear();
    buildIssueItemInfo(configuration, timestamp, itemInfoQueue, futureList, crawlerTaskExecutor);
    log.info("Creating item information and adding in queue");
    jiraProjectCache.keySet().forEach(key -> {
      Map<String, String> metadata = new HashMap<>();
      metadata.put(CONTENT_TYPE, JiraContentType.PROJECT.getType());
      ItemInfo itemInfo = createItemInfo(_PROJECT + key, metadata);
      itemInfoQueue.add(itemInfo);
    });

  }

  /**
   * Method for building Issue Item Info.
   *
   * @param configuration Input Parameter
   * @param timestamp     Input Parameter
   * @return Item Info Queue
   */
  private void buildIssueItemInfo(JiraSourceConfig configuration, long timestamp,
                                  Queue<ItemInfo> itemInfoQueue, List<Future<Boolean>> futureList,
                                  ExecutorService crawlerTaskExecutor) {
    log.info("Building issue item information");
    StringBuilder jql = createIssueFilterCriteria(configuration, timestamp);
    int total;
    int startAt = 0;
    try {
      do {
        List<IssueBean> issueList = new ArrayList<>();
        SearchResults searchIssues = getAllIssues(jql, startAt, configuration);
        issueList.addAll(searchIssues.getIssues());
        total = searchIssues.getTotal();
        startAt += searchIssues.getIssues().size();
        futureList.add(crawlerTaskExecutor.submit(
                () -> addItemsToQueue(issueList, itemInfoQueue), false));
      } while (startAt < total);
    } catch (RuntimeException ex) {
      log.error("An exception has occurred while fetching"
                      + " issue entity information , Error: {}", ex.getMessage());
      throw new BadRequestException(ex.getMessage(), ex);
    }
  }

  /**
   * Add items to queue.
   * @param issueList Issue list.
   * @param itemInfoQueue Item info queue.
   */
  private void addItemsToQueue(List<IssueBean> issueList, Queue<ItemInfo> itemInfoQueue) {
    issueList.forEach(issue -> {
      Map<String, String> issueMetadata = new HashMap<>();
      if (Objects.nonNull(((LinkedTreeMap) issue.getFields().get(PROJECT)).get(KEY))) {
        issueMetadata.put(PROJECT_KEY,
                ((LinkedTreeMap) issue.getFields().get(PROJECT)).get(KEY).toString());
      }
      if (Objects.nonNull(((LinkedTreeMap) issue.getFields().get(PROJECT)).get(NAME))) {
        issueMetadata.put(PROJECT_NAME,
                ((LinkedTreeMap) issue.getFields().get(PROJECT)).get(NAME).toString());
      }

      long created = 0;
      if (Objects.nonNull(issue.getFields()) && issue.getFields().get(CREATED)
              .toString().length() >= 23) {
        String charSequence = issue.getFields().get(CREATED).toString().substring(0, 23) + "Z";
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence);
        new Date(offsetDateTime.toInstant().toEpochMilli());
        created = offsetDateTime.toEpochSecond() * 1000;
      }
      issueMetadata.put(CREATED, String.valueOf(created));

      long updated = 0;
      if (issue.getFields().get(UPDATED).toString().length() >= 23) {
        String charSequence = issue.getFields().get(UPDATED).toString().substring(0, 23) + "Z";
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence);
        new Date(offsetDateTime.toInstant().toEpochMilli());
        updated = offsetDateTime.toEpochSecond() * 1000;
      }
      issueMetadata.put(UPDATED, String.valueOf(updated));

      issueMetadata.put(ISSUE_KEY, issue.getKey());
      issueMetadata.put(CONTENT_TYPE, JiraContentType.ISSUE.getType());
      String id = _ISSUE + issueMetadata.get(PROJECT_KEY) + "-" + issue.getKey();

      itemInfoQueue.add(createItemInfo(id, issueMetadata));

      if (Objects.nonNull(issueMetadata.get(PROJECT_KEY)) && !jiraProjectCache
              .containsKey(issueMetadata.get(PROJECT_KEY))) {
        jiraProjectCache.put(issueMetadata.get(PROJECT_KEY), LIVE);
      }

    });
  }

  /**
   * Method to get Issues.
   *
   * @param jql           input parameter.
   * @param startAt       the start at
   * @param configuration input parameter.
   * @return InputStream input stream
   */
  public SearchResults getAllIssues(StringBuilder jql, int startAt,
                                    JiraSourceConfig configuration) {
    SearchResults results = null;
    HttpResponse<JsonNode> response;
    com.mashape.unirest.request.HttpRequest request;
    try {
      if (configuration.getAuthType().equals(BASIC.trim())) {
        AddressValidation.validateInetAddress(AddressValidation
                .getInetAddress(configuration.getAccountUrl()));

        request = Unirest.get(configuration.getAccountUrl() + REST_API_SEARCH)
                .basicAuth(configuration.getJiraId(), configuration.getJiraCredential())
                .header(ACCEPT, Application_JSON)
                .queryString(MAX_RESULTS, FIFTY)
                .queryString(START_AT, startAt)
                .queryString(JQL_FIELD, jql)
                .queryString(EXPAND_FIELD, EXPAND_VALUE);

        response = request.asJson();
        /*appLog.info("Search result api call response is: {}",
                new Gson().toJson(response, com.mashape.unirest.http.HttpResponse.class));*/
        if (response.getStatus() == BAD_RESPONSE) {
          if (Objects.nonNull(response.getBody())
                  && Objects.nonNull(response.getBody().getObject())) {
            log.error("An exception has occurred while getting"
                            + " response from Jira search API {} ",
                    response.getBody().getObject().get(ERR_MSG).toString());
            throw new BadRequestException(response.getBody().getObject().get(ERR_MSG).toString());
          }
        }
        Gson gson = new GsonBuilder().create();
        results = gson.fromJson(response.getBody().getObject().toString(), SearchResults.class);
      } else if (configuration.getAuthType().equals(OAUTH2)) {
        int retryCount = 0;
        boolean shouldContinue = Boolean.TRUE;
        while (shouldContinue && (retryCount < RETRY_ATTEMPT)) {
          request = Unirest.get(REST_API_SEARCH)
                  .header(ACCEPT, Application_JSON)
                  .header(HttpHeaders.AUTHORIZATION, String
                          .format("%s %s", Constants.TOKEN_TYPE, JiraOauthConfig.accessToken))
                  .queryString(MAX_RESULT, FIFTY)
                  .queryString(START_AT, startAt)
                  .queryString(JQL_FIELD, jql)
                  .queryString(EXPAND_FIELD, EXPAND_VALUE);
          response = request.asJson();
          if (response.getStatus() == TOKEN_EXPIRED) {
            JiraOauthConfig.changeAccessAndRefreshToken(configuration);
            retryCount++;
          } else if (response.getStatus() == SUCCESS_RESPONSE) {
            Gson gson = new GsonBuilder().create();
            results = gson.fromJson(response.getBody().getObject().toString(), SearchResults.class);
            shouldContinue = Boolean.FALSE;
          } else {
            if (Objects.nonNull(response.getBody().getObject())) {
              log.error("An exception has occurred while "
                              + "getting response from Jira search API  {}",
                      response.getBody().getObject().get(ERR_MSG).toString());
              throw new BadRequestException(response.getBody().getObject().get(ERR_MSG).toString());
            }
          }
        }
      } else {
        log.error("Auth type provided in configuration is invalid");
        throw new BadRequestException("Auth type provided in configuration is invalid");
      }
    } catch (UnirestException e) {
      log.error("An exception has occurred while connecting to Jira search API: {}", e.getMessage());
      throw new BadRequestException(e.getMessage(), e);
    }
    return results;
  }

  /**
   * Method for creating Issue Filter Criteria.
   *
   * @param configuration Input Parameter
   * @param ts            Input Parameter
   * @return String Builder
   */
  private StringBuilder createIssueFilterCriteria(JiraSourceConfig configuration, long ts) {

    log.info("Creating issue filter criteria");
    if (!CollectionUtils.isEmpty(configHelper.getProjectKeyFilter(configuration))) {
      validateProjectFilters(configuration);
    }
    StringBuilder jiraQl = new StringBuilder(UPDATED + GREATER_THAN_EQUALS + ts);
    if (!CollectionUtils.isEmpty(configHelper.getProjectKeyFilter(configuration))) {
      jiraQl.append(PROJECT_IN).append(configHelper.getProjectKeyFilter(configuration).stream()
                      .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
              .append(CLOSING_ROUND_BRACKET);
    }
    if (!CollectionUtils.isEmpty(configHelper.getIssueTypeFilter(configuration))) {
      jiraQl.append(ISSUE_TYPE_ID).append(configHelper.getIssueTypeFilter(configuration).stream()
                      .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
              .append(CLOSING_ROUND_BRACKET);
    }
    if (!CollectionUtils.isEmpty(configHelper.getIssueStatusFilter(configuration))) {
      jiraQl.append(STATUS_IN).append(configHelper.getIssueStatusFilter(configuration).stream()
                      .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
              .append(CLOSING_ROUND_BRACKET);
    }
    log.info("Created issue filter criteria JiraQl query: {}", jiraQl);
    return jiraQl;
  }

  /**
   * Method to build message input stream.
   *
   * @param text input parameter.
   * @return ByteArrayInputStream input stream
   */
  public final InputStream buildTextInputStream(String text) {
    if (text!=null && !text.isEmpty()) {
      text = EMPTY_STRING;
    }
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    return new ByteArrayInputStream(bytes);
  }

  /**
   * Gets issue.
   *
   * @param issueKey      the item info
   * @param configuration the configuration
   * @return the issue
   */
  public String getIssue(String issueKey, JiraSourceConfig configuration) {
    log.info("Started to fetch issue information");
    Queue<Integer> waitTimeQueue = new ConcurrentLinkedQueue<>(waitTimeList);

    while(true) {
      String url = configuration.getAccountUrl() + REST_API_FETCH_ISSUE + "/" + issueKey;
      log.info("Issue Fetching api call request is : {}", url);
      try {
        return restTemplate.getForEntity(url, String.class).getBody();
      } catch (ClientAuthorizationRequiredException ex) {

        log.error("Failed to execute the rest call ",ex);

      }
    }
  }


  /**
   * Gets issue search api.
   *
   * @param configuration the configuration
   * @return the issue search api
   */
  public SearchResults getIssueSearchApi(JiraConfigHelper configuration, String jql) {
    //TODO: Fill this method body
    return null;
  }

  /**
   * Method for Validating Project Filters.
   *
   * @param configuration Input Parameter
   */
  private void validateProjectFilters(JiraSourceConfig configuration) {
    log.info("Validating project filters");
    List<String> badFilters = new ArrayList<>();
    Pattern regex = Pattern.compile("[^A-Z0-9]");
    configHelper.getProjectKeyFilter(configuration).forEach(projectFilter -> {
      Matcher matcher = regex.matcher(projectFilter);
      if (matcher.find() || projectFilter.length() <= 1 || projectFilter.length() > 10) {
        badFilters.add(projectFilter);
      }
    });
    if (!badFilters.isEmpty()) {
      String filters = String.join("\"" + badFilters + "\"", ", ");
      log.error("One or more invalid project keys found in filter configuration: {}", badFilters);
      throw new BadRequestException(BAD_REQUEST_EXCEPTION
              + filters);
    }
  }

  /**
   * Method for creating Item Info.
   *
   * @param key       Input Parameter
   * @param metadata  Input Parameter
   * @return Item Info
   */
  private ItemInfo createItemInfo(String key, Map<String, String> metadata) {
    long eventTime = Date.from(Instant.now()).getTime();
      return JiraItemInfo.builder().withEventTime(eventTime)
              .withId(metadata.get(ISSUE_KEY))
              .withItemId(key)
              .withMetadata(metadata)
              .withProject(metadata.get(PROJECT_KEY))
              .withIssueType(metadata.get(CONTENT_TYPE))
              .build();
  }

  /**
   * Re-Test connection boolean.
   *
   * @param configuration the configuration
   * @return the boolean
   */
  public static boolean reTestConnection(JiraConfigHelper configuration) {
    Queue<Integer> waitTimeQueue = new ConcurrentLinkedQueue<>(waitTimeList);
    boolean shouldContinue = Boolean.TRUE;
    while (shouldContinue) {
      try {
        //TODO: replace below line
//        getMyselfApi(configuration).getCurrentUser(EMPTY_STRING);
        shouldContinue = Boolean.FALSE;
      } catch (RuntimeException ex) {
        if (/*ex.status() == RATE_LIMIT &&*/ !CollectionUtils.isEmpty(waitTimeQueue)) {
          log.error("Re-testing connection due to error: {}", ex.getMessage());
          String waitTime = String.valueOf(waitTimeQueue.remove());
          handleThrottling(waitTime, Boolean.TRUE);
        } else {
          log.error("An exception has occurred while "
                  + "re-testing connection {}", ex.getMessage());
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Method to handle Slack API Throttling.
   *
   * @param waitTime    input parameter.
   * @param resumeCrawl input parameter.
   */
  public static void handleThrottling(String waitTime, boolean resumeCrawl) {
    int seconds = Integer.parseInt(waitTime);
    long retryAfter = seconds * 1000L;
    try {
      Thread.sleep(retryAfter);
    } catch (InterruptedException interruptedException) {
      //TODO: Better exception handling required here
      log.error("Thread interrupted while handling throttling");
      throw new RuntimeException(interruptedException);
    }
  }

}
