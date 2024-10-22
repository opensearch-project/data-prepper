package org.opensearch.dataprepper.plugins.source.saas.jira.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

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
    void setup(){
        issueBean =  new IssueBean();
    }

    @Test
    public void testInitialization(){
        assertNotNull(issueBean);
    }

    @Test
    public void testNull(){
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
    public void testStringSettersAndGetters(){
        String self = "selfTest";
        String key = "keyTest";
        String id = "idTest";
        String expand = "expandTest";

        issueBean.setExpand(expand);
        assert(issueBean.getExpand().equals(expand));;
        issueBean.setId(id);
        assert(issueBean.getId().equals(id));
        issueBean.setSelf(self);
        assert(issueBean.getSelf().equals(self));
        issueBean.setKey(key);
        assert(issueBean.getKey().equals(key));
    }

    @Test
    public void testMapSettersAndGetters(){

        issueBean.setRenderedFields(renderedFieldsTestObject);
        assert(issueBean.getRenderedFields().equals(renderedFieldsTestObject));
        issueBean.setProperties(propertiesTestObject);
        assert(issueBean.getProperties().equals(propertiesTestObject));
        issueBean.setNames(namesTestObject);
        assert(issueBean.getNames().equals(namesTestObject));
        issueBean.setFields(fieldsTestObject);
        assert(issueBean.getFields().equals(fieldsTestObject));
    }

}
