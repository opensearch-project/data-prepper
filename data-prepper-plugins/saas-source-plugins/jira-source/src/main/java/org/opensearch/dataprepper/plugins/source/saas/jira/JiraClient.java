package org.opensearch.dataprepper.plugins.source.saas.jira;

import com.google.gson.internal.LinkedTreeMap;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasClient;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourceConfig;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.SaasWorkerProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.Item;
import org.opensearch.dataprepper.plugins.source.saas.crawler.model.ItemInfo;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.IssueBean;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.IssueItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ASSIGNEE_FIELD;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BROWSE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.BULLETLIST;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CODEBLOCK;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.COMMENT_TEXT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CONTENT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CREATED;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.CREATOR;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.DESCCRIPTION_FIELD;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.DISPLAY_NAME;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.EMPTY_STRING;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.HEADING;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_KEY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_STATUS;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ISSUE_SUMMARY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.LISTITEM_TEXT;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.NAME;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.NOT_ASSIGNED;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.ORDEREDLIST;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PARAGRAPH;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_KEY;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.PROJECT_NAME;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.SPACE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.TYPE;
import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.UPDATED;

/**
 * This class represents a Jira client.
 */
@Named
public class JiraClient implements SaasClient {

    private static final Logger log = LoggerFactory.getLogger(JiraClient.class);

    private final JiraService service;
    private JiraConfiguration configuration;
    private final JiraIterator jiraIterator;
    private long lastPollTime;

    public JiraClient(JiraService service, JiraIterator jiraIterator) {
        this.service = service;
        this.jiraIterator = jiraIterator;
    }



    @Override
    public Optional<Item> getItem(ItemInfo itemInfo) {
        IssueBean issue = service.getIssue(itemInfo.getId(), configuration);
        return buildIssueItem(issue, itemInfo, configuration);
    }

    private Optional<Item> buildIssueItem(IssueBean issue, ItemInfo itemInfo, JiraConfiguration configuration) {
        log.info("Fetching metadata for {} issue entity", itemInfo.getItemId());
        long created = 0;
        if (Objects.nonNull(issue.getFields()) && issue.getFields().get(CREATED)
                .toString().length() >= 23) {
            String charSequence = issue.getFields().get(CREATED).toString().substring(0, 23) + "Z";
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence);
            new Date(offsetDateTime.toInstant().toEpochMilli());
            created = offsetDateTime.toEpochSecond() * 1000;
        }
        long updated = 0;
        if (issue.getFields().get(UPDATED).toString().length() >= 23) {
            String charSequence = issue.getFields().get(UPDATED).toString().substring(0, 23) + "Z";
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(charSequence);
            new Date(offsetDateTime.toInstant().toEpochMilli());
            updated = offsetDateTime.toEpochSecond() * 1000;
        }
        String assignee = NOT_ASSIGNED;
        if (Objects.nonNull(issue.getFields().get(ASSIGNEE_FIELD))
                && Objects.nonNull(((LinkedTreeMap) issue.getFields().get(ASSIGNEE_FIELD))
                .get(DISPLAY_NAME))) {
            assignee = ((LinkedTreeMap) issue.getFields().get(ASSIGNEE_FIELD))
                    .get(DISPLAY_NAME).toString();
        }
        List contentList = new ArrayList();
        if (issue.getFields().containsKey(DESCCRIPTION_FIELD)
                && Objects.nonNull(issue.getFields().get(DESCCRIPTION_FIELD))
                && ((LinkedTreeMap) issue.getFields().get(DESCCRIPTION_FIELD)).containsKey(CONTENT)) {
            contentList = Arrays.asList(((LinkedTreeMap) issue.getFields().get(
                    DESCCRIPTION_FIELD)).get(CONTENT));
        }
        StringBuilder documentBodyString = createDocumentBody(contentList);
        String inputStreamString = !documentBodyString.toString().trim().isEmpty()
                ? documentBodyString.toString() : (String) issue.getFields().get(ISSUE_SUMMARY);
        InputStream inputStream = service.buildTextInputStream(inputStreamString);
        Map<String, Object> customMetadata = null;// service.getCustomFieldMapping(issue, configuration);
        Boolean isCustomMetadataPresent = false; //isCustomMetadataPresent(customMetadata, itemInfo.getItemId());
        String projectName = itemInfo.getMetadata().get(PROJECT_NAME);
        String projectKey = itemInfo.getMetadata().get(PROJECT_KEY);
        String url = configuration.getJiraAccountUrl() + BROWSE + issue.getKey();
        List<String> authors = new ArrayList<>();
        if (Objects.nonNull((issue.getFields().get(CREATOR)))
                && Objects.nonNull(((LinkedTreeMap) issue.getFields().get(CREATOR)).get(
                DISPLAY_NAME))) {
            authors = Arrays.asList(((LinkedTreeMap) issue.getFields().get(CREATOR)).get(
                    DISPLAY_NAME).toString());
        }
        String status = ((LinkedTreeMap) issue.getFields().get(ISSUE_STATUS)).get(NAME).toString();

        return Optional.ofNullable(
                IssueItem.builder().configuration(configuration).id(itemInfo.getItemId()).issue(issue)
                        .createdAt(created).updatedAt(updated).url(url)
                        .projectKey(projectKey)
                        .status(status)
                        .assignee(assignee).projectName(projectName).authors(authors)
                        .inputStream(inputStream)
                        .isCustomMetadataPresent(isCustomMetadataPresent)
                        .customMetadata(customMetadata)
                        .build());
    }


    /**
     * Method to create document body.
     *
     * @param contentList input parameter
     * @return StringBuilder
     */
    private StringBuilder createDocumentBody(List contentList) {

        StringBuilder documentBodyString = new StringBuilder("");
        for (int i = 0; i < contentList.size(); i++) {
            int size = ((ArrayList) contentList.get(i)).size();
            for (int j = 0; j < size; j++) {
                if (((LinkedTreeMap) ((ArrayList) contentList.get(i)).get(j)).containsKey(TYPE)) {
                    Object typeObject = ((LinkedTreeMap) ((ArrayList) contentList.get(i)).get(j)).get(TYPE);
                    if (typeObject.equals(PARAGRAPH) || typeObject.equals(CODEBLOCK)
                            || typeObject.equals(HEADING) || typeObject.equals(BULLETLIST)
                            || typeObject.equals(ORDEREDLIST)) {

                        List contentItem = Arrays.asList(((LinkedTreeMap) ((ArrayList) contentList
                                .get(i)).get(j)).get(CONTENT));

                        for (int contentItemListIndex = 0; contentItemListIndex < contentItem.size();
                             contentItemListIndex++) {
                            if (((ArrayList) contentItem.get(contentItemListIndex)) != null) {
                                for (int contentItemIndex = 0; contentItemIndex < ((ArrayList) contentItem
                                        .get(contentItemListIndex)).size(); contentItemIndex++) {

                                    if (typeObject.equals(BULLETLIST) || typeObject.equals(ORDEREDLIST)) {

                                        if (((LinkedTreeMap) ((ArrayList) contentItem.get(contentItemListIndex))
                                                .get(contentItemIndex)).containsKey(TYPE)
                                                && ((LinkedTreeMap) ((ArrayList) contentItem.get(contentItemListIndex))
                                                .get(contentItemIndex)).get(TYPE)
                                                .equals(LISTITEM_TEXT)) {

                                            ArrayList paragraphList = (ArrayList) ((LinkedTreeMap) ((ArrayList)
                                                    contentItem.get(contentItemListIndex)).get(contentItemIndex))
                                                    .get(CONTENT);
                                            for (int t = 0; t < paragraphList.size(); t++) {
                                                ArrayList contentData = (ArrayList) ((LinkedTreeMap)
                                                        paragraphList.get(t)).get(CONTENT);
                                                for (int p = 0; p < contentData.size(); p++) {
                                                    Object data = ((LinkedTreeMap) (contentData).get(p)).get(COMMENT_TEXT);
                                                    documentBodyString.append(data + SPACE);
                                                }
                                            }
                                        }
                                    } else {
                                        if (((LinkedTreeMap) ((ArrayList) contentItem.get(contentItemListIndex))
                                                .get(contentItemIndex)).containsKey(TYPE)
                                                && ((LinkedTreeMap) ((ArrayList) contentItem.get(contentItemListIndex))
                                                .get(contentItemIndex))
                                                .containsKey(COMMENT_TEXT)) {
                                            documentBodyString.append(((LinkedTreeMap) ((ArrayList)
                                                    contentItem.get(contentItemListIndex)).get(contentItemIndex))
                                                    .get(COMMENT_TEXT) + SPACE);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return documentBodyString;
    }

    @Override
    public Iterator<ItemInfo> listItems() {
        jiraIterator.initialize(lastPollTime);
        return jiraIterator;
    }

    @Override
    public void setConfiguration(SaasSourceConfig configuration) {
        this.configuration = JiraConfiguration.of((JiraSourceConfig) configuration);
        this.jiraIterator.setSourceConfig(configuration);
    }

    @Override
    public void setLastPollTime(long lastPollTime) {
        log.info("Setting the lastPollTime: {}", lastPollTime);
        this.lastPollTime = lastPollTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer<Record<Event>> buffer) {
        List<String> itemIds = state.getItemIds();
        Map<String, Object> keyAttributes = state.getKeyAttributes();
        String project = (String)keyAttributes.get(PROJECT_KEY);
        long eventTime = state.getExportStartTime();
        //TODO: parallize this work
        for(String itemId : itemIds) {
            ItemInfo itemInfo = JiraItemInfo.builder()
                    .withId(itemId)
                    .withProject(project)
                    .withEventTime(eventTime).build();
            Optional<Item> item = getItem(itemInfo);
            if(item.isPresent()) {
                Item jiraItem = item.get();
                //TODO: write to buffer
                //buffer.write(jiraItem.getDocumentTitle(), 1000);
            }
        }
    }
}
