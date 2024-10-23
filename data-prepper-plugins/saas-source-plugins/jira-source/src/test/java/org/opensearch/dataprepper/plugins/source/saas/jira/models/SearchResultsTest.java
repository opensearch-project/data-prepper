package org.opensearch.dataprepper.plugins.source.saas.jira.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class SearchResultsTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private Map<String, String> names;

    @Mock
    private Map<String, JsonTypeBean> schema;

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
        assertNull(searchResults.getStartAt());
        assertNull(searchResults.getMaxResults());
        assertNull(searchResults.getTotal());
        assertNull(searchResults.getIssues());
        assertNull(searchResults.getWarningMessages());
        assertNull(searchResults.getNames());
        assertNull(searchResults.getSchema());
    }

    @Test
    public void testGetters() throws JsonProcessingException {
        String expand = "expandTest";
        Integer startAt = 1;
        Integer maxResults = 100;
        Integer total = 10;
        List<IssueBean> testIssues = new ArrayList<>();
        IssueBean issue1 = new IssueBean();
        IssueBean issue2 = new IssueBean();
        issue1.setId("issue 1");
        issue2.setId("issue 2");
        testIssues.add(issue1);
        testIssues.add(issue2);
        List<String> testWarnings = Arrays.asList("Warning1", "Warning2");

        Map<String, Object> map = new HashMap<>();
        map.put("expand", expand);
        map.put("startAt", startAt);
        map.put("maxResults", maxResults);
        map.put("total", total);
        map.put("issues", testIssues);
        map.put("warningMessages", testWarnings);
        map.put("names", names);
        map.put("schema", schema);

        String jsonString = objectMapper.writeValueAsString(map);

        searchResults = objectMapper.readValue(jsonString, SearchResults.class);

        assertEquals(searchResults.getExpand(), expand);
        assertEquals(searchResults.getStartAt(), startAt);
        assertEquals(searchResults.getMaxResults(), maxResults);
        assertEquals(searchResults.getTotal(), total);
        assertEquals(searchResults.getWarningMessages(), testWarnings);
        assertEquals(searchResults.getNames(), names);
        assertEquals(searchResults.getSchema(), schema);


        List<IssueBean> returnedIssues = searchResults.getIssues();
        assertNotNull(returnedIssues);
        assertEquals(testIssues.size(), returnedIssues.size());

        // Compare each issue's properties
        for (int i = 0; i < testIssues.size(); i++) {
            IssueBean originalIssue = testIssues.get(i);
            IssueBean returnedIssue = returnedIssues.get(i);

            assertEquals(originalIssue.getId(), returnedIssue.getId());
        }
    }

    @Test
    public void testToString() throws JsonProcessingException {
        String state = "{\"expand\": \"same\"}";
        searchResults = objectMapper.readValue(state, SearchResults.class);
        String jsonString = searchResults.toString();
        System.out.print(jsonString);
        assertTrue(jsonString.contains("expand: same"));
        assertTrue(jsonString.contains("startAt: null"));
        assertTrue(jsonString.contains("maxResults: null"));
        assertTrue(jsonString.contains("total: null"));
        assertTrue(jsonString.contains("ISSUE"));
        assertTrue(jsonString.contains("warningMessages"));
        assertTrue(jsonString.contains("name"));
        assertTrue(jsonString.contains("schema"));
    }

}
