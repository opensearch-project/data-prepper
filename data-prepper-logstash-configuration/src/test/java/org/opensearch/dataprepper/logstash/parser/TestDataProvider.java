package org.opensearch.dataprepper.logstash.parser;

import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;
import org.opensearch.dataprepper.logstash.model.LogstashPlugin;
import org.opensearch.dataprepper.logstash.model.LogstashAttribute;
import org.opensearch.dataprepper.logstash.model.LogstashAttributeValue;
import org.opensearch.dataprepper.logstash.model.LogstashValueType;

import java.util.UUID;
import java.util.Random;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.LinkedHashMap;


class TestDataProvider {
    static final String RANDOM_STRING_1 = UUID.randomUUID().toString();
    static final String RANDOM_STRING_2 = UUID.randomUUID().toString();
    static final String RANDOM_VALUE = String.valueOf(new Random().nextInt(1000));

    public static LogstashConfiguration configData() {
        List<LogstashPlugin> pluginContextList = new LinkedList<>(Collections.singletonList(pluginWithOneArrayContextAttributeData()));
        Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();
        pluginSections.put(LogstashPluginType.INPUT, pluginContextList);

        return LogstashConfiguration.builder().pluginSections(pluginSections).build();
    }

    public static List<LogstashPlugin> pluginSectionData() {

        List<LogstashPlugin> pluginContextList = new LinkedList<>(Collections.singletonList(pluginWithOneArrayContextAttributeData()));
        Map<LogstashPluginType, List<LogstashPlugin>> pluginSections = new LinkedHashMap<>();
        pluginSections.put(LogstashPluginType.INPUT, pluginContextList);

        return pluginContextList;
    }

    public static LogstashPlugin pluginWithNoAttributeData() {
        List<LogstashAttribute> logstashAttributeList = new LinkedList<>();

        return LogstashPlugin.builder().pluginName(TestDataProvider.RANDOM_STRING_1).attributes(logstashAttributeList).build();
    }

    public static LogstashPlugin pluginWithOneArrayContextAttributeData() {
        List<LogstashAttribute> logstashAttributeList = new LinkedList<>();
        logstashAttributeList.add(TestDataProvider.attributeWithArrayTypeValueData());

        return LogstashPlugin.builder().pluginName(TestDataProvider.RANDOM_STRING_1).attributes(logstashAttributeList).build();
    }

    public static LogstashPlugin pluginWithMorThanOneArrayContextAttributeData() {
        List<LogstashAttribute> logstashAttributeList = new LinkedList<>();
        logstashAttributeList.add(TestDataProvider.attributeWithArrayTypeValueData());
        logstashAttributeList.add(TestDataProvider.attributeWithArrayTypeValueData());

        return LogstashPlugin.builder().pluginName(TestDataProvider.RANDOM_STRING_1).attributes(logstashAttributeList).build();
    }

    public static LogstashAttribute attributeWithArrayTypeValueData() {
        List<Object> values = new LinkedList<>(Arrays.asList(TestDataProvider.RANDOM_STRING_1, TestDataProvider.RANDOM_STRING_2));

        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder().
                attributeValueType(LogstashValueType.ARRAY).value(values).build();

        return LogstashAttribute.builder()
                .attributeName(TestDataProvider.RANDOM_STRING_1).attributeValue(logstashAttributeValue).build();
    }

    public static LogstashAttribute attributeWithHashTypeValueData() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put(TestDataProvider.RANDOM_STRING_1, TestDataProvider.RANDOM_STRING_2);

        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder().
                attributeValueType(LogstashValueType.HASH).value(values).build();

        return LogstashAttribute.builder()
                .attributeName(TestDataProvider.RANDOM_STRING_1).attributeValue(logstashAttributeValue).build();
    }

    public static LogstashAttribute attributeWithNumberTypeValueData() {
        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder().
                attributeValueType(LogstashValueType.NUMBER).value(Double.valueOf(TestDataProvider.RANDOM_VALUE)).build();
        return LogstashAttribute.builder()
                .attributeName(TestDataProvider.RANDOM_STRING_1).attributeValue(logstashAttributeValue).build();
    }

    public static LogstashAttribute attributeWithBareWordTypeValueData() {
        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder().
                attributeValueType(LogstashValueType.BAREWORD).value(RANDOM_STRING_2).build();
        return LogstashAttribute.builder()
                .attributeName(RANDOM_STRING_1).attributeValue(logstashAttributeValue).build();
    }

    public static LogstashAttribute attributeWithStringTypeValueData() {
        LogstashAttributeValue logstashAttributeValue = LogstashAttributeValue.builder().
                attributeValueType(LogstashValueType.STRING).value(RANDOM_STRING_2).build();
        return LogstashAttribute.builder()
                .attributeName(RANDOM_STRING_1).attributeValue(logstashAttributeValue).build();
    }

    public static List<String> arrayData() {
        return Arrays.asList(RANDOM_STRING_1, RANDOM_STRING_2);
    }

    public static Map<String, Object> hashEntriesArrayData() {
        Map<String, Object> hashentry = new HashMap<>();
        hashentry.put(RANDOM_STRING_1, new LinkedList<>(Arrays.asList(RANDOM_STRING_1, RANDOM_STRING_2)));

        return hashentry;
    }

    public static Map<String, Object> hashEntriesStringData() {
        Map<String, Object> hashentry = new HashMap<>();
        hashentry.put(RANDOM_STRING_1, RANDOM_STRING_2);

        return hashentry;
    }

    public static List<String> hashEntryArrayData() {
        return Arrays.asList(RANDOM_STRING_1, RANDOM_STRING_2);
    }

    public static String hashEntryStringData() {
        return RANDOM_STRING_2;
    }

}