/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class SearchResultsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private SearchResults searchResults;

    @BeforeEach
    public void setUp() throws JsonProcessingException {
        String state = "{}";
        searchResults = objectMapper.readValue(state, SearchResults.class);
    }

    @Test
    public void testConstructor() {
        assertNotNull(searchResults);

        assertNull(searchResults.getExpand());
        assertNull(searchResults.getMaxResults());
        assertEquals(searchResults.getIsLast(), false);
        assertNull(searchResults.getNextPageToken());
        assertNull(searchResults.getIssues());
    }

    @Test
    public void testGetters() throws JsonProcessingException {
        String expand = "expandTest";
        String nextPageToken = "tokenTest";
        Integer maxResults = 100;
        Boolean isLast = true;
        List<IssueBean> testIssues = new ArrayList<>();
        IssueBean issue1 = new IssueBean();
        IssueBean issue2 = new IssueBean();
        issue1.setId("issue 1");
        issue2.setId("issue 2");
        testIssues.add(issue1);
        testIssues.add(issue2);


        Map<String, Object> searchResultsMap = new HashMap<>();
        searchResultsMap.put("expand", expand);
        searchResultsMap.put("maxResults", maxResults);
        searchResultsMap.put("nextPageToken", nextPageToken);
        searchResultsMap.put("isLast", isLast);
        searchResultsMap.put("issues", testIssues);
        

        String jsonString = objectMapper.writeValueAsString(searchResultsMap);

        searchResults = objectMapper.readValue(jsonString, SearchResults.class);

        assertEquals(searchResults.getExpand(), expand);
        assertEquals(searchResults.getMaxResults(), maxResults);
        assertEquals(searchResults.getNextPageToken(), nextPageToken);
        assertEquals(searchResults.getIsLast(), isLast);

        List<IssueBean> returnedIssues = searchResults.getIssues();
        assertNotNull(returnedIssues);
        assertEquals(testIssues.size(), returnedIssues.size());

        for (int i = 0; i < testIssues.size(); i++) {
            IssueBean originalIssue = testIssues.get(i);
            IssueBean returnedIssue = returnedIssues.get(i);

            assertEquals(originalIssue.getId(), returnedIssue.getId());
        }
    }


}
