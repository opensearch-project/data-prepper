package org.opensearch.dataprepper.plugins.source.jira;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.jira.exception.BadRequestException;
import org.opensearch.dataprepper.plugins.source.jira.exception.UnAuthorizedException;
import org.opensearch.dataprepper.plugins.source.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.jira.models.SearchResults;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraContentType;
import org.opensearch.dataprepper.plugins.source.source_crawler.model.ItemInfo;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.inject.Named;
import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BAD_REQUEST_EXCEPTION;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CLOSING_ROUND_BRACKET;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CONTENT_TYPE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.DELIMITER;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.EXPAND_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.EXPAND_VALUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.FIFTY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.GREATER_THAN_EQUALS;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ISSUE_TYPE_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.JQL_FIELD;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.LIVE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.MAX_RESULT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.OAUTH2;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PREFIX;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_KEY;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PROJECT_NAME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.REST_API_FETCH_ISSUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.REST_API_SEARCH;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.RETRY_ATTEMPT_SLEEP_TIME;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.START_AT;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.STATUS_IN;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SUCCESS_RESPONSE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.SUFFIX;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.TOKEN_EXPIRED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.UPDATED;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants._ISSUE;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants._PROJECT;


/**
 * Service class for interactive external Atlassian jira SaaS service and fetch required details using their rest apis.
 */

@Slf4j
@Named
public class JiraService {

    private static final String ISSUES_REQUESTED = "issuesRequested";
    private static final String TICKET_FETCH_LATENCY_TIMER = "ticketFetchLatency";
    private static final String SEARCH_CALL_LATENCY_TIMER = "searchCallLatency";
    private static final String SEARCH_RESULTS_FOUND = "searchResultsFound";
    private static final Map<String, String> jiraProjectCache = new ConcurrentHashMap<>();
    private final RestTemplate restTemplate;
    private final JiraAuthConfig authConfig;
    private final JiraSourceConfig jiraSourceConfig;
    private final Counter issuesRequestedCounter;
    private final Counter searchResultsFoundCounter;
    private final Timer ticketFetchLatencyTimer;
    private final Timer searchCallLatencyTimer;
    private final PluginMetrics jiraPluginMetrics = PluginMetrics.fromNames("jiraService", "aws");


    public JiraService(RestTemplate restTemplate,
                       JiraSourceConfig jiraSourceConfig,
                       JiraAuthConfig authConfig) {
        this.restTemplate = restTemplate;
        this.jiraSourceConfig = jiraSourceConfig;

        issuesRequestedCounter = jiraPluginMetrics.counter(ISSUES_REQUESTED);
        ticketFetchLatencyTimer = jiraPluginMetrics.timer(TICKET_FETCH_LATENCY_TIMER);
        searchResultsFoundCounter = jiraPluginMetrics.counter(SEARCH_RESULTS_FOUND);
        searchCallLatencyTimer = jiraPluginMetrics.timer(SEARCH_CALL_LATENCY_TIMER);
        this.authConfig = authConfig;

    }

    /**
     * Get jira entities.
     *
     * @param configuration       the configuration.
     * @param timestamp           timestamp.
     * @param itemInfoQueue       Item info queue.
     * @param futureList          Future list.
     * @param crawlerTaskExecutor Executor service.
     */
    public void getJiraEntities(JiraSourceConfig configuration, Instant timestamp,
                                Queue<ItemInfo> itemInfoQueue, List<Future<Boolean>> futureList,
                                ExecutorService crawlerTaskExecutor) {
        log.info("Started to fetch entities");
        jiraProjectCache.clear();
        searchForNewTicketsAndAddToQueue(configuration, timestamp, itemInfoQueue, futureList, crawlerTaskExecutor);
        log.trace("Creating item information and adding in queue");
        jiraProjectCache.keySet().forEach(key -> {
            Map<String, Object> metadata = new HashMap<>();
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
     */
    private void searchForNewTicketsAndAddToQueue(JiraSourceConfig configuration, Instant timestamp,
                                                  Queue<ItemInfo> itemInfoQueue, List<Future<Boolean>> futureList,
                                                  ExecutorService crawlerTaskExecutor) {
        log.trace("Looking for Add/Modified tickets with a Search API call");
        StringBuilder jql = createIssueFilterCriteria(configuration, timestamp);
        int total;
        int startAt = 0;
        try {
            do {
                SearchResults searchIssues = getAllIssues(jql, startAt, configuration);
                List<IssueBean> issueList = new ArrayList<>(searchIssues.getIssues());
                total = searchIssues.getTotal();
                startAt += searchIssues.getIssues().size();
                futureList.add(crawlerTaskExecutor.submit(
                        () -> addItemsToQueue(issueList, itemInfoQueue), false));
            } while (startAt < total);
            searchResultsFoundCounter.increment(total);
            log.info("Number of tickets found in search api call: {}", total);
        } catch (RuntimeException ex) {
            log.error("An exception has occurred while fetching"
                    + " issue entity information , Error: {}", ex.getMessage());
            throw new BadRequestException(ex.getMessage(), ex);
        }
    }

    /**
     * Add items to queue.
     *
     * @param issueList     Issue list.
     * @param itemInfoQueue Item info queue.
     */
    private void addItemsToQueue(List<IssueBean> issueList, Queue<ItemInfo> itemInfoQueue) {
        issueList.forEach(issue -> {
            Map<String, Object> issueMetadata = new HashMap<>();
            if (Objects.nonNull(((Map) issue.getFields().get(PROJECT)).get(KEY))) {
                issueMetadata.put(PROJECT_KEY,
                        ((Map) issue.getFields().get(PROJECT)).get(KEY).toString());
            }
            if (Objects.nonNull(((Map) issue.getFields().get(PROJECT)).get(NAME))) {
                issueMetadata.put(PROJECT_NAME,
                        ((Map) issue.getFields().get(PROJECT)).get(NAME).toString());
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
                jiraProjectCache.put((String) issueMetadata.get(PROJECT_KEY), LIVE);
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
    @Timed(SEARCH_CALL_LATENCY_TIMER)
    public SearchResults getAllIssues(StringBuilder jql, int startAt,
                                      JiraSourceConfig configuration) {

        String url = configuration.getAccountUrl() + REST_API_SEARCH;
        if (configuration.getAuthType().equals(OAUTH2)) {
            url = authConfig.getUrl() + REST_API_SEARCH;
        }

        URI uri = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam(MAX_RESULT, FIFTY)
                .queryParam(START_AT, startAt)
                .queryParam(JQL_FIELD, jql)
                .queryParam(EXPAND_FIELD, EXPAND_VALUE)
                .buildAndExpand().toUri();
        return invokeRestApi(uri, SearchResults.class).getBody();
    }

    /**
     * Gets issue.
     *
     * @param issueKey the item info
     * @return the issue
     */
    @Timed(TICKET_FETCH_LATENCY_TIMER)
    public String getIssue(String issueKey) {
        issuesRequestedCounter.increment();
        String url = authConfig.getUrl() + REST_API_FETCH_ISSUE + "/" + issueKey;
        URI uri = UriComponentsBuilder.fromHttpUrl(url).buildAndExpand().toUri();
        return invokeRestApi(uri, String.class).getBody();
    }

    private <T> ResponseEntity<T> invokeRestApi(URI uri, Class<T> responseType) {

        int retryCount = 0;
        while (retryCount < RETRY_ATTEMPT) {
            ResponseEntity<T> responseEntity = restTemplate.getForEntity(uri, responseType);
            int statusCode = responseEntity.getStatusCode().value();
            if (statusCode == TOKEN_EXPIRED) {
                authConfig.renewCredentials();
                try {
                    Thread.sleep(RETRY_ATTEMPT_SLEEP_TIME.get(retryCount) * 1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Sleep in the retry attempt got interrupted", e);
                }
                retryCount++;
            } else if (statusCode == SUCCESS_RESPONSE) {
                return responseEntity;
            } else {
                if (Objects.nonNull(responseEntity.getBody())) {
                    log.error("An exception has occurred while "
                                    + "getting response from Jira search API  {}",
                            responseEntity.getBody());
                    throw new BadRequestException(responseEntity.getBody().toString());
                }
            }
        }
        throw new UnAuthorizedException("Exceeded max retry attempts");
    }

    /**
     * Method for creating Issue Filter Criteria.
     *
     * @param configuration Input Parameter
     * @param ts            Input Parameter
     * @return String Builder
     */
    private StringBuilder createIssueFilterCriteria(JiraSourceConfig configuration, Instant ts) {

        log.info("Creating issue filter criteria");
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectKeyFilter(configuration))) {
            validateProjectFilters(configuration);
        }
        StringBuilder jiraQl = new StringBuilder(UPDATED + GREATER_THAN_EQUALS + ts.toEpochMilli());
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getProjectKeyFilter(configuration))) {
            jiraQl.append(PROJECT_IN).append(JiraConfigHelper.getProjectKeyFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueTypeFilter(configuration))) {
            jiraQl.append(ISSUE_TYPE_IN).append(JiraConfigHelper.getIssueTypeFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        if (!CollectionUtils.isEmpty(JiraConfigHelper.getIssueStatusFilter(configuration))) {
            jiraQl.append(STATUS_IN).append(JiraConfigHelper.getIssueStatusFilter(configuration).stream()
                            .collect(Collectors.joining(DELIMITER, PREFIX, SUFFIX)))
                    .append(CLOSING_ROUND_BRACKET);
        }
        log.info("Created issue filter criteria JiraQl query: {}", jiraQl);
        return jiraQl;
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
        JiraConfigHelper.getProjectKeyFilter(configuration).forEach(projectFilter -> {
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
     * @param key      Input Parameter
     * @param metadata Input Parameter
     * @return Item Info
     */
    private ItemInfo createItemInfo(String key, Map<String, Object> metadata) {
        return JiraItemInfo.builder().withEventTime(Instant.now())
                .withId((String) metadata.get(ISSUE_KEY))
                .withItemId(key)
                .withMetadata(metadata)
                .withProject((String) metadata.get(PROJECT_KEY))
                .withIssueType((String) metadata.get(CONTENT_TYPE))
                .build();
    }

}
