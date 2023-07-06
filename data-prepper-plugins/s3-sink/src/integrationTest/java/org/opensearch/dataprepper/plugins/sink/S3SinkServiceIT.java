/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.avro.AvroOutputCodec;
import org.opensearch.dataprepper.plugins.codec.avro.AvroOutputCodecConfig;
import org.opensearch.dataprepper.plugins.codec.csv.CsvOutputCodec;
import org.opensearch.dataprepper.plugins.codec.csv.CsvOutputCodecConfig;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.codec.newline.NewlineDelimitedOutputCodec;
import org.opensearch.dataprepper.plugins.codec.newline.NewlineDelimitedOutputConfig;
import org.opensearch.dataprepper.plugins.sink.accumulator.BufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.sink.accumulator.ObjectKey;
import org.opensearch.dataprepper.plugins.sink.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ThresholdOptions;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3SinkServiceIT {

    private static final String PATH_PREFIX = UUID.randomUUID().toString() + "/%{yyyy}/%{MM}/%{dd}/";
    private static final ObjectMapper mapper = new ObjectMapper();
    private S3Client s3Client;
    private String bucketName;
    private static final int numberOfRecords = 2;

    private BufferFactory bufferFactory;
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private ThresholdOptions thresholdOptions;
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;
    @Mock
    private ObjectKeyOptions objectKeyOptions;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter snapshotSuccessCounter;
    @Mock
    private Counter snapshotFailedCounter;
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private DistributionSummary s3ObjectSizeSummary;

    private OutputCodec codec;

    @Mock
    NewlineDelimitedOutputConfig newlineDelimitedOutputConfig;
    @Mock
    private JsonOutputCodecConfig jsonOutputCodecConfig;
    private CsvOutputCodecConfig csvOutputCodecConfig;
    private AvroOutputCodecConfig avroOutputCodecConfig;


    @BeforeEach
    public void setUp() {
        String s3region = System.getProperty("tests.s3ink.region");

        s3Client = S3Client.builder().region(Region.of(s3region)).build();
        bucketName = System.getProperty("tests.s3sink.bucket");

        bufferFactory = new InMemoryBufferFactory();

        when(objectKeyOptions.getNamePattern()).thenReturn("elb-log-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(objectKeyOptions.getPathPrefix()).thenReturn(PATH_PREFIX);
        when(s3SinkConfig.getBucketName()).thenReturn(bucketName);
        when(s3SinkConfig.getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(thresholdOptions.getEventCount()).thenReturn(2);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("2mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.parse("PT3M"));
        when(s3SinkConfig.getThresholdOptions()).thenReturn(thresholdOptions);

        lenient().when(pluginMetrics.counter(S3SinkService.OBJECTS_SUCCEEDED)).thenReturn(snapshotSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.OBJECTS_FAILED)).thenReturn(snapshotFailedCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_SUCCESS)).
                thenReturn(numberOfRecordsSuccessCounter);
        lenient().when(pluginMetrics.counter(S3SinkService.NUMBER_OF_RECORDS_FLUSHED_TO_S3_FAILED)).
                thenReturn(numberOfRecordsFailedCounter);
        lenient().when(pluginMetrics.summary(S3SinkService.S3_OBJECTS_SIZE)).thenReturn(s3ObjectSizeSummary);
    }

    @Test
    void verify_flushed_object_count_into_s3_bucket() {
        configureNewLineCodec();
        int s3ObjectCountBeforeIngest = gets3ObjectCount();
        S3SinkService s3SinkService = createObjectUnderTest();
        s3SinkService.output(setEventQueue());
        int s3ObjectCountAfterIngest = gets3ObjectCount();
        assertThat(s3ObjectCountAfterIngest, equalTo(s3ObjectCountBeforeIngest + 1));
    }

    void configureNewLineCodec() {
        codec = new NewlineDelimitedOutputCodec(newlineDelimitedOutputConfig);
        when(newlineDelimitedOutputConfig.getExcludeKeys()).thenReturn(new ArrayList<>());
    }

    @Test
    void verify_flushed_records_into_s3_bucketNewLine() {
        configureNewLineCodec();
        S3SinkService s3SinkService = createObjectUnderTest();
        Collection<Record<Event>> recordsData = setEventQueue();

        s3SinkService.output(recordsData);
        String objectData = new String(getS3Object());

        int count = 0;
        String[] objectDataArr = objectData.split("\r\n");
        for (Record<Event> recordData : recordsData) {
            String objectRecord = recordData.getData().toJsonString();
            assertThat(objectDataArr[count], CoreMatchers.containsString(objectRecord));
            count++;
        }
    }

    private S3SinkService createObjectUnderTest() {
        return new S3SinkService(s3SinkConfig, bufferFactory, codec, s3Client, "Tag", pluginMetrics);
    }

    private int gets3ObjectCount() {
        int s3ObjectCount = 0;
        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucketName).prefix(getPathPrefix())
                .build();
        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();
        s3ObjectCount = objects.size();

        return s3ObjectCount;
    }

    private byte[] getS3Object() {

        ListObjectsRequest listObjects = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(getPathPrefix())
                .build();
        ListObjectsResponse res = s3Client.listObjects(listObjects);
        List<S3Object> objects = res.contents();

        S3Object object = objects.get(objects.size() - 1);

        String objectKey = object.key();
        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .key(objectKey)
                .bucket(bucketName).build();

        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
        return objectBytes.asByteArray();
    }

    private String getPathPrefix() {
        return ObjectKey.buildingPathPrefix(s3SinkConfig);
    }

    private Collection<Record<Event>> setEventQueue() {
        final Collection<Record<Event>> jsonObjects = new LinkedList<>();
        for (int i = 0; i < 2; i++)
            jsonObjects.add(createRecord());
        return jsonObjects;
    }

    private static Record<Event> createRecord() {
        final Set<String> testTags = Set.of("tag1");
        final EventMetadata defaultEventMetadata = DefaultEventMetadata.builder().
                withEventType(EventType.LOG.toString()).
                withTags(testTags).build();
        Map<String, Object> json = generateJson(testTags);
        final JacksonEvent event = JacksonLog.builder().withData(json).withEventMetadata(defaultEventMetadata).build();
        event.setEventHandle(mock(EventHandle.class));
        return new Record<>(event);
    }

    private static Map<String, Object> generateJson(Set<String> testTags) {
        final Map<String, Object> jsonObject = new LinkedHashMap<>();
        for (int i = 0; i < 2; i++) {
            jsonObject.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
        jsonObject.put(UUID.randomUUID().toString(), Arrays.asList(UUID.randomUUID().toString(),
                UUID.randomUUID().toString(), UUID.randomUUID().toString()));
        jsonObject.put("Tag", testTags.toArray());
        return jsonObject;
    }
    private static Record getRecord(int index) {
        List<HashMap> recordList = generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, String> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }

    private static List<GenericRecord> createAvroRecordsList(byte[] avroData) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
        List<GenericRecord> actualRecords = new ArrayList<>();

        while (stream.hasNext()) {
            GenericRecord avroRecord = stream.next();
            actualRecords.add(avroRecord);
        }
        return actualRecords;
    }

    private static Object decodeOutputIfEncoded(Object encodedActualOutput) {
        if (encodedActualOutput instanceof Utf8) {
            byte[] utf8Bytes = encodedActualOutput.toString().getBytes(StandardCharsets.UTF_8);
            return new String(utf8Bytes, StandardCharsets.UTF_8);
        } else {
            return encodedActualOutput;
        }
    }

    private static Schema parseSchema() {
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();
    }

    private static List<String> csvHeader() {
        List<String> header = new ArrayList<>();
        header.add("name");
        header.add("age");
        return header;
    }
    void configureJsonCodec() {
        codec = new JsonOutputCodec(jsonOutputCodecConfig);
        when(jsonOutputCodecConfig.getExcludeKeys()).thenReturn(new ArrayList<>());
    }

    @Test
    void verify_flushed_records_into_s3_bucket_Json() throws IOException {
        configureJsonCodec();
        S3SinkService s3SinkService = createObjectUnderTest();
        Collection<Record<Event>> recordsData = getRecordList();

        s3SinkService.output(recordsData);

        int index = 0;
        List<HashMap> expectedRecords = generateRecords(numberOfRecords);
        JsonNode jsonNode = mapper.readTree(getS3Object());
        for (JsonNode element : jsonNode) {
            Set<String> keys = expectedRecords.get(index).keySet();
            Map<String, Object> actualMap = new HashMap<>();
            for (String key : keys) {
                actualMap.put(key, element.get(key).asText());
            }
            assertThat(expectedRecords.get(index), Matchers.equalTo(actualMap));
            index++;

        }
    }

    @Test
    void verify_flushed_records_into_s3_bucket_Avro() throws IOException {
        configureAvroCodec();
        S3SinkService s3SinkService = createObjectUnderTest();
        Collection<Record<Event>> recordsData = getRecordList();

        s3SinkService.output(recordsData);

        List<GenericRecord> actualRecords = createAvroRecordsList(getS3Object());
        int index = 0;
        for (final GenericRecord actualRecord : actualRecords) {

            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getSchema(), notNullValue());

            Map expectedMap = generateRecords(numberOfRecords).get(index);
            Map actualMap = new HashMap();
            for (Schema.Field field : actualRecord.getSchema().getFields()) {
                Object decodedActualOutput = decodeOutputIfEncoded(actualRecord.get(field.name()));
                actualMap.put(field.name(), decodedActualOutput);
            }
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
    }

    private void configureAvroCodec() {
        avroOutputCodecConfig = new AvroOutputCodecConfig();
        avroOutputCodecConfig.setSchema(parseSchema().toString());
        avroOutputCodecConfig.setExcludeKeys(new ArrayList<>());
        codec = new AvroOutputCodec(avroOutputCodecConfig);

    }

    @Test
    void verify_flushed_records_into_s3_bucket_Csv() {
        configureCsvCodec();
        S3SinkService s3SinkService = createObjectUnderTest();
        Collection<Record<Event>> recordsData = getRecordList();

        s3SinkService.output(recordsData);
        String csvData = new String(getS3Object());
        StringReader stringReader = new StringReader(csvData);
        CSVReader csvReader = new CSVReaderBuilder(stringReader).build();

        try {
            String[] line;
            int index = 0;
            int headerIndex;
            List<String> headerList = csvHeader();
            List<HashMap> expectedRecords = generateRecords(numberOfRecords);
            while ((line = csvReader.readNext()) != null) {
                if (index == 0) {
                    headerIndex = 0;
                    for (String value : line) {
                        assertThat(headerList.get(headerIndex), Matchers.equalTo(value));
                        headerIndex++;
                    }
                } else {
                    headerIndex = 0;
                    for (String value : line) {
                        assertThat(expectedRecords.get(index - 1).get(headerList.get(headerIndex)), Matchers.equalTo(value));
                        headerIndex++;
                    }
                }
                index++;
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        } finally {
            try {
                csvReader.close();
                stringReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void configureCsvCodec() {
        csvOutputCodecConfig = new CsvOutputCodecConfig();
        csvOutputCodecConfig.setHeader(csvHeader());
        csvOutputCodecConfig.setExcludeKeys(new ArrayList<>());
        codec = new CsvOutputCodec(csvOutputCodecConfig);
    }
    private Collection<Record<Event>> getRecordList() {
        final Collection<Record<Event>> recordList = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++)
            recordList.add(getRecord(i));
        return recordList;
    }
}
