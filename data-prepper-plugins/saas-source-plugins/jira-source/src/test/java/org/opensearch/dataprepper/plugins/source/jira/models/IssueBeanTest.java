package org.opensearch.dataprepper.plugins.source.jira.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class IssueBeanTest {

    @Mock
    private Map<String, Object> renderedFieldsTestObject;

    @Mock
    private Map<String, Object> propertiesTestObject;

    @Mock
    private Map<String, String> namesTestObject;

    @Mock
    private Map<String, Object> fieldsTestObject;

    private IssueBean issueBean;

    @BeforeEach
    void setup() {
        issueBean = new IssueBean();
    }

    @Test
    public void testInitialization() {
        assertNotNull(issueBean);
    }

    @Test
    public void testNull() {
        assertNull(issueBean.getExpand());
        assertNull(issueBean.getId());
        assertNull(issueBean.getSelf());
        assertNull(issueBean.getKey());
        assertNull(issueBean.getRenderedFields());
        assertNull(issueBean.getProperties());
        assertNull(issueBean.getNames());
        assertNull(issueBean.getFields());
    }

    @Test
    void testNullCases() {
        assertNull(issueBean.getProject());
        assertNull(issueBean.getProjectName());
        assertEquals(issueBean.getUpdatedTimeMillis(), 0);
    }

    @Test
    public void testStringSettersAndGetters() {
        String self = "selfTest";
        String key = "keyTest";
        String id = "idTest";
        String expand = "expandTest";

        issueBean.setExpand(expand);
        assertEquals(issueBean.getExpand(), expand);
        issueBean.setId(id);
        assertEquals(issueBean.getId(), id);
        issueBean.setSelf(self);
        assertEquals(issueBean.getSelf(), self);
        issueBean.setKey(key);
        assertEquals(issueBean.getKey(), key);
    }

    @Test
    public void testMapSettersAndGetters() {

        issueBean.setRenderedFields(renderedFieldsTestObject);
        assertEquals(issueBean.getRenderedFields(), renderedFieldsTestObject);
        issueBean.setProperties(propertiesTestObject);
        assertEquals(issueBean.getProperties(), propertiesTestObject);
        issueBean.setNames(namesTestObject);
        assertEquals(issueBean.getNames(), namesTestObject);
        issueBean.setFields(fieldsTestObject);
        assertEquals(issueBean.getFields(), fieldsTestObject);
    }

}
