package org.opensearch.dataprepper.logstash.parser;

import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.opensearch.dataprepper.logstash.LogstashParser;
import org.opensearch.dataprepper.logstash.exception.LogstashParsingException;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

class LogstashVisitorTest {

    private static LogstashVisitor logstashVisitor;

    @BeforeEach
    void createObjectUnderTest() {
        logstashVisitor = spy(new LogstashVisitor());
    }

    @Mock
    private LogstashParser.ConfigContext configContextMock = mock(LogstashParser.ConfigContext.class);
    @Mock
    private LogstashParser.Plugin_sectionContext pluginSectionMock = mock(LogstashParser.Plugin_sectionContext.class);
    @Mock
    private LogstashParser.Plugin_typeContext pluginTypeContextMock = mock(LogstashParser.Plugin_typeContext.class);
    @Mock
    private LogstashParser.Branch_or_pluginContext branchOrPluginContextMock = mock(LogstashParser.Branch_or_pluginContext.class);
    @Mock
    private LogstashParser.BranchContext branchContextMock = mock(LogstashParser.BranchContext.class);
    @Mock
    private LogstashParser.PluginContext pluginContextMock = mock(LogstashParser.PluginContext.class);
    @Mock
    private LogstashParser.NameContext nameContextMock = mock(LogstashParser.NameContext.class);
    @Mock
    private LogstashParser.AttributesContext attributesContextMock = mock(LogstashParser.AttributesContext.class);
    @Mock
    private LogstashParser.AttributeContext attributeContextMock = mock(LogstashParser.AttributeContext.class);
    @Mock
    private LogstashParser.ValueContext valueContextMock = mock(LogstashParser.ValueContext.class);
    @Mock
    private LogstashParser.ArrayContext arrayContextMock = mock(LogstashParser.ArrayContext.class);
    @Mock
    private LogstashParser.ValueContext arrayValueContextMock1 = mock(LogstashParser.ValueContext.class);
    @Mock
    private LogstashParser.ValueContext arrayValueContextMock2 = mock(LogstashParser.ValueContext.class);
    @Mock
    private LogstashParser.HashContext hashContextMock = mock(LogstashParser.HashContext.class);
    @Mock
    private LogstashParser.HashentriesContext hashentriesContextMock = mock(LogstashParser.HashentriesContext.class);
    @Mock
    private TerminalNodeImpl terminalNodeMock = mock(TerminalNodeImpl.class);
    @Mock
    private LogstashParser.HashentryContext hashentryContextMock = mock(LogstashParser.HashentryContext.class);
    @Mock
    private LogstashParser.HashnameContext hashnameContextMock = mock(LogstashParser.HashnameContext.class);


    @Test
    void visit_config_return_logstash_configuration_object_test() {
        given(configContextMock.plugin_section()).willReturn(Collections.singletonList(pluginSectionMock));
        given(pluginSectionMock.plugin_type()).willReturn(pluginTypeContextMock);
        given(pluginTypeContextMock.getText()).willReturn("input");
        given(pluginSectionMock.branch_or_plugin()).willReturn(Collections.singletonList(branchOrPluginContextMock));

        Mockito.doReturn(TestDataProvider.pluginWithOneArrayContextAttributeData()).when(logstashVisitor).visitBranch_or_plugin(branchOrPluginContextMock);

        LogstashConfiguration actualLogstashConfiguration = (LogstashConfiguration) logstashVisitor.visitConfig(configContextMock);
        LogstashConfiguration expectedLogstashConfiguration = TestDataProvider.configData();

        assertThat(actualLogstashConfiguration.getPluginSection(LogstashPluginType.INPUT).size(),
                equalTo(expectedLogstashConfiguration.getPluginSection(LogstashPluginType.INPUT).size()));
        Mockito.verify(logstashVisitor, Mockito.times(1)).visitPlugin_section(pluginSectionMock);
    }

    @Test
    void visit_plugin_section_test() {
        given(pluginSectionMock.plugin_type()).willReturn(pluginTypeContextMock);
        given(pluginTypeContextMock.getText()).willReturn("input");

        List<LogstashParser.Branch_or_pluginContext> branch_or_pluginContextList = new LinkedList<>(
                Collections.singletonList(branchOrPluginContextMock)
        );
        given(pluginSectionMock.branch_or_plugin()).willReturn(branch_or_pluginContextList);
        given(branchOrPluginContextMock.getChild(0)).willReturn(pluginContextMock);
        given(branchOrPluginContextMock.plugin()).willReturn(pluginContextMock);

        Mockito.doReturn(TestDataProvider.pluginWithOneArrayContextAttributeData()).when(logstashVisitor).visitPlugin(pluginContextMock);

        List<LogstashPlugin> actualPluginSections = (List<LogstashPlugin>) logstashVisitor.visitPlugin_section(pluginSectionMock);
        List<LogstashPlugin> expectedPluginSections = TestDataProvider.pluginSectionData();

        System.out.println(actualPluginSections);
        System.out.println(expectedPluginSections);

        assertThat(actualPluginSections.size(), equalTo(expectedPluginSections.size()));
        for (int i = 0; i < expectedPluginSections.size(); i++)
            assertThat(actualPluginSections.get(i).getPluginName(), equalTo(expectedPluginSections.get(i).getPluginName()));
    }

    @Test
    void visit_config_with_unsupported_section_name_throws_logstash_parsing_exception_test() {
        given(configContextMock.plugin_section()).willReturn(Collections.singletonList(pluginSectionMock));
        given(pluginSectionMock.plugin_type()).willReturn(pluginTypeContextMock);
        given(pluginTypeContextMock.getText()).willReturn("inputt");

        Exception exception = assertThrows(LogstashParsingException.class, () ->
            logstashVisitor.visitConfig(configContextMock));

        String expectedMessage = "only input, filter and output plugin sections are supported.";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void visit_branch_or_plugin_with_branch_throws_logstash_parsing_exception_test() {
        given(branchOrPluginContextMock.getChild(0)).willReturn(branchContextMock);

        Exception exception = assertThrows(LogstashParsingException.class, () ->
            logstashVisitor.visitBranch_or_plugin(branchOrPluginContextMock));

        String expectedMessage = "conditionals are not supported";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void visit_branch_or_plugin_returns_logstash_plugin_test() {
        given(branchOrPluginContextMock.getChild(0)).willReturn(pluginContextMock);
        given(branchOrPluginContextMock.plugin()).willReturn(pluginContextMock);
        given(pluginContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(pluginContextMock.attributes()).willReturn(attributesContextMock);

        List<LogstashParser.AttributeContext> attributeContexts = new LinkedList<>(Collections.singletonList(attributeContextMock));
        given(pluginContextMock.attributes()).willReturn(attributesContextMock);
        given(attributesContextMock.attribute()).willReturn(attributeContexts);

        Mockito.doReturn(TestDataProvider.attributeWithArrayTypeValueData()).when(logstashVisitor).visitAttribute(attributeContextMock);

        LogstashPlugin actualLogstashPlugin = (LogstashPlugin) logstashVisitor.visitBranch_or_plugin(branchOrPluginContextMock);
        LogstashPlugin expectedLogstashPlugin = TestDataProvider.pluginWithOneArrayContextAttributeData();

        assertThat(actualLogstashPlugin.getPluginName(), equalTo(expectedLogstashPlugin.getPluginName()));
        for (int i = 0; i < actualLogstashPlugin.getAttributes().size(); i++) {
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeName(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeName()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType()));
        }

    }

    @Test
    void visit_plugin_with_no_attribute_test() {
        given(pluginContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(pluginContextMock.attributes()).willReturn(attributesContextMock);

        Mockito.doReturn(TestDataProvider.arrayData()).when(logstashVisitor).visitArray(arrayContextMock);

        LogstashPlugin actualLogstashPlugin = (LogstashPlugin) logstashVisitor.visitPlugin(pluginContextMock);
        LogstashPlugin expectedLogstashPlugin = TestDataProvider.pluginWithNoAttributeData();

        assertThat(actualLogstashPlugin.getPluginName(), equalTo(expectedLogstashPlugin.getPluginName()));
        assertThat(actualLogstashPlugin.getAttributes().size(), equalTo(expectedLogstashPlugin.getAttributes().size()));
    }

    @Test
    void visit_plugin_with_one_array_context_attribute_test() {
        given(pluginContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(pluginContextMock.attributes()).willReturn(attributesContextMock);

        List<LogstashParser.AttributeContext> attributeContexts = new LinkedList<>(Collections.singletonList(attributeContextMock));

        given(attributesContextMock.attribute()).willReturn(attributeContexts);
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getChild(0)).willReturn(arrayContextMock);
        given(valueContextMock.array()).willReturn(arrayContextMock);

        Mockito.doReturn(TestDataProvider.arrayData()).when(logstashVisitor).visitArray(arrayContextMock);

        LogstashPlugin actualLogstashPlugin = (LogstashPlugin) logstashVisitor.visitPlugin(pluginContextMock);
        LogstashPlugin expectedLogstashPlugin = TestDataProvider.pluginWithOneArrayContextAttributeData();

        assertThat(actualLogstashPlugin.getPluginName(), equalTo(expectedLogstashPlugin.getPluginName()));
        assertThat(actualLogstashPlugin.getAttributes().size(), equalTo(expectedLogstashPlugin.getAttributes().size()));

        for (int i = 0; i < actualLogstashPlugin.getAttributes().size(); i++) {

            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeName(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeName()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType()));
        }
    }

    @Test
    void visit_plugin_with_more_than_one_array_context_attribute_test() {
        given(pluginContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(pluginContextMock.attributes()).willReturn(attributesContextMock);

        List<LogstashParser.AttributeContext> attributeContexts = new LinkedList<>();
        attributeContexts.add(attributeContextMock);
        attributeContexts.add(attributeContextMock);
        given(attributesContextMock.attribute()).willReturn(attributeContexts);

        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getChild(0)).willReturn(arrayContextMock);
        given(valueContextMock.array()).willReturn(arrayContextMock);

        Mockito.doReturn(TestDataProvider.arrayData()).when(logstashVisitor).visitArray(arrayContextMock);

        LogstashPlugin actualLogstashPlugin = (LogstashPlugin) logstashVisitor.visitPlugin(pluginContextMock);
        LogstashPlugin expectedLogstashPlugin = TestDataProvider.pluginWithMorThanOneArrayContextAttributeData();

        assertThat(actualLogstashPlugin.getPluginName(), equalTo(expectedLogstashPlugin.getPluginName()));
        assertThat(actualLogstashPlugin.getAttributes().size(), equalTo(expectedLogstashPlugin.getAttributes().size()));

        for (int i = 0; i < actualLogstashPlugin.getAttributes().size(); i++) {

            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeName(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeName()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getValue()));
            assertThat(actualLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType(),
                    equalTo(expectedLogstashPlugin.getAttributes().get(i).getAttributeValue().getAttributeValueType()));
        }
    }

    @Test
    void visit_attribute_with_value_type_array_returns_correct_logstash_attribute_test() {
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getChild(0)).willReturn(arrayContextMock);
        given(valueContextMock.array()).willReturn(arrayContextMock);

        List<LogstashParser.ValueContext> valueContextList = new LinkedList<>(Arrays.asList(arrayValueContextMock1, arrayValueContextMock2));
        given(arrayContextMock.value()).willReturn(valueContextList);
        given(arrayValueContextMock1.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(arrayValueContextMock2.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);

        LogstashAttribute actualLogstashAttribute = (LogstashAttribute) logstashVisitor.visitAttribute(attributeContextMock);
        LogstashAttribute expectedLogstashAttribute = TestDataProvider.attributeWithArrayTypeValueData();

        assertThat(actualLogstashAttribute.getAttributeName(),
                equalTo(expectedLogstashAttribute.getAttributeName()));
        assertThat(actualLogstashAttribute.getAttributeValue().getValue(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getValue()));
        assertThat(actualLogstashAttribute.getAttributeValue().getAttributeValueType(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getAttributeValueType()));
    }

    @Test
    void visit_attribute_with_value_type_hash_returns_correct_logstash_attribute_test() {
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getChild(0)).willReturn(hashContextMock);
        given(valueContextMock.hash()).willReturn(hashContextMock);
        given(hashContextMock.hashentries()).willReturn(hashentriesContextMock);

        Mockito.doReturn(TestDataProvider.hashEntriesStringData()).when(logstashVisitor).visitHashentries(hashentriesContextMock);

        LogstashAttribute actualLogstashAttribute = (LogstashAttribute) logstashVisitor.visitAttribute(attributeContextMock);
        LogstashAttribute expectedLogstashAttribute = TestDataProvider.attributeWithHashTypeValueData();

        assertThat(actualLogstashAttribute.getAttributeName(),
                equalTo(expectedLogstashAttribute.getAttributeName()));
        assertThat(actualLogstashAttribute.getAttributeValue().getValue(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getValue()));
        assertThat(actualLogstashAttribute.getAttributeValue().getAttributeValueType(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getAttributeValueType()));
    }

    @Test
    void visit_attribute_with_value_type_plugin_throws_logstash_parsing_exception_test() {
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getChild(0)).willReturn(pluginContextMock);

        Exception exception = assertThrows(LogstashParsingException.class, () -> logstashVisitor.visitAttribute(attributeContextMock));

        String expectedMessage = "plugins are not supported in an attribute";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

    @Test
    void visit_attribute_with_value_type_number_returns_correct_logstash_attribute_test() {
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.NUMBER()).willReturn(terminalNodeMock);
        given(valueContextMock.getText()).willReturn(TestDataProvider.RANDOM_VALUE);
        given(valueContextMock.NUMBER().toString()).willReturn(TestDataProvider.RANDOM_VALUE);

        LogstashAttribute actualLogstashAttribute = (LogstashAttribute) logstashVisitor.visitAttribute(attributeContextMock);
        LogstashAttribute expectedLogstashAttribute = TestDataProvider.attributeWithNumberTypeValueData();

        assertThat(actualLogstashAttribute.getAttributeName(),
                equalTo(expectedLogstashAttribute.getAttributeName()));
        assertThat(actualLogstashAttribute.getAttributeValue().getValue(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getValue()));
        assertThat(actualLogstashAttribute.getAttributeValue().getAttributeValueType(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getAttributeValueType()));
    }

    @Test
    void visit_attribute_with_value_type_bare_word_returns_correct_logstash_attribute_test() {
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.BAREWORD()).willReturn(terminalNodeMock);
        given(valueContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);
        given(valueContextMock.BAREWORD().toString()).willReturn(TestDataProvider.RANDOM_STRING_2);

        LogstashAttribute actualLogstashAttribute = (LogstashAttribute) logstashVisitor.visitAttribute(attributeContextMock);
        LogstashAttribute expectedLogstashAttribute = TestDataProvider.attributeWithBareWordTypeValueData();


        assertThat(actualLogstashAttribute.getAttributeName(),
                equalTo(expectedLogstashAttribute.getAttributeName()));
        assertThat(actualLogstashAttribute.getAttributeValue().getValue(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getValue()));
        assertThat(actualLogstashAttribute.getAttributeValue().getAttributeValueType(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getAttributeValueType()));

    }

    @Test
    void visit_attribute_with_value_type_string_returns_correct_logstash_attribute_test() {
        given(attributeContextMock.name()).willReturn(nameContextMock);
        given(nameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(attributeContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.STRING()).willReturn(terminalNodeMock);
        given(valueContextMock.getText()).willReturn("\"" + TestDataProvider.RANDOM_STRING_2 + "\"");
        given(valueContextMock.STRING().toString()).willReturn("\"" + TestDataProvider.RANDOM_STRING_2 + "\"");

        LogstashAttribute actualLogstashAttribute = (LogstashAttribute) logstashVisitor.visitAttribute(attributeContextMock);
        LogstashAttribute expectedLogstashAttribute = TestDataProvider.attributeWithStringTypeValueData();

        assertThat(actualLogstashAttribute.getAttributeName(),
                equalTo(expectedLogstashAttribute.getAttributeName()));
        assertThat(actualLogstashAttribute.getAttributeValue().getValue(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getValue()));
        assertThat(actualLogstashAttribute.getAttributeValue().getAttributeValueType(),
                equalTo(expectedLogstashAttribute.getAttributeValue().getAttributeValueType()));

    }

    @Test
    void visit_array_with_empty_array_returns_empty_list_test() {
        given(arrayContextMock.value()).willReturn(Collections.emptyList());
        Object actualList = logstashVisitor.visitArray(arrayContextMock);

        assertThat(actualList, equalTo(Collections.emptyList()));
    }

    @Test
    void visit_array_with_singleton_array_returns_list_df_size_one_test() {
        given(valueContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(arrayContextMock.value()).willReturn(Collections.singletonList(valueContextMock));

        Object actualList = logstashVisitor.visitArray(arrayContextMock);

        assertThat(actualList, equalTo(Collections.singletonList(TestDataProvider.RANDOM_STRING_1)));
    }

    @Test
    void visit_array_with_array_of_size_more_than_one_returns_list_of_size_more_than_one_test() {
        List<LogstashParser.ValueContext> valueContextList = new LinkedList<>(Arrays.asList(arrayValueContextMock1, arrayValueContextMock2));

        given(arrayContextMock.value()).willReturn(valueContextList);
        given(arrayValueContextMock1.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(arrayValueContextMock2.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);

        Object actualList = logstashVisitor.visitArray(arrayContextMock);

        assertThat(actualList, equalTo(TestDataProvider.arrayData()));
    }

    @Test
    void visit_hash_entries_with_string_value_returns_map_of_hash_entries_test() {
        List<LogstashParser.HashentryContext> hashentryContextList = new LinkedList<>(
                Collections.singletonList(hashentryContextMock));

        given(hashentriesContextMock.hashentry()).willReturn(hashentryContextList);
        given(hashentryContextMock.hashname()).willReturn(hashnameContextMock);
        given(hashnameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(hashentryContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);

        Object actualMap = logstashVisitor.visitHashentries(hashentriesContextMock);

        assertThat(actualMap, equalTo(TestDataProvider.hashEntriesStringData()));
    }

    @Test
    void visit_hash_entries_with_array_value_returns_map_of_hash_entries_test() {
        List<LogstashParser.HashentryContext> hashentryContextList = new LinkedList<>(
                Collections.singletonList(hashentryContextMock));

        given(hashentriesContextMock.hashentry()).willReturn(hashentryContextList);
        given(hashentryContextMock.hashname()).willReturn(hashnameContextMock);
        given(hashnameContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(hashentryContextMock.value()).willReturn(valueContextMock);
        given(hashentryContextMock.value().getChild(0)).willReturn(arrayContextMock);
        given(valueContextMock.array()).willReturn(arrayContextMock);

        Mockito.doReturn(TestDataProvider.arrayData()).when(logstashVisitor).visitArray(arrayContextMock);

        Object actualMap = logstashVisitor.visitHashentries(hashentriesContextMock);
        Object expectedMap = TestDataProvider.hashEntriesArrayData();


        assertThat(actualMap, equalTo(expectedMap));
    }

    @Test
    void visit_hash_entry_with_value_type_string_returns_string_object_test() {
        given(hashentryContextMock.value()).willReturn(valueContextMock);
        given(valueContextMock.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);

        Object actualValue = logstashVisitor.visitHashentry(hashentryContextMock);

        assertThat(actualValue, equalTo(TestDataProvider.hashEntryStringData()));
    }

    @Test
    void visit_hash_entry_with_array_value_type_returns_list_test() {
        final List<LogstashParser.ValueContext> linkedListValuesMock = new LinkedList<>(
                Arrays.asList(arrayValueContextMock1, arrayValueContextMock2)
        );

        given(hashentryContextMock.value()).willReturn(valueContextMock);
        given(hashentryContextMock.value().getChild(0)).willReturn(arrayContextMock);
        given(valueContextMock.array()).willReturn(arrayContextMock);
        given(arrayContextMock.value()).willReturn(linkedListValuesMock);
        given(arrayValueContextMock1.getText()).willReturn(TestDataProvider.RANDOM_STRING_1);
        given(arrayValueContextMock2.getText()).willReturn(TestDataProvider.RANDOM_STRING_2);

        Object actualList = logstashVisitor.visitHashentry(hashentryContextMock);

        assertThat(actualList, equalTo(TestDataProvider.hashEntryArrayData()));
    }

}