/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ParquetOutputCodecTest {
    private static final String FILE_NAME = "parquet-data";
    private static final String expectedSchemaString = "{\"type\":\"record\",\"name\":\"sesblog\",\"fields\":[{\"name\":" +
            "\"eventType\",\"type\":\"string\"},{\"name\":\"event_date\",\"type\":{\"type\":\"int\",\"logicalType\":" +
            "\"date\",\"precision\":\"10\",\"scale\":\"2\"}},{\"name\":\"col2\",\"type\":\"string\"},{\"name\":\"tags\"," +
            "\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"col3\",\"type\":\"string\"}]}";
    private static final String inputString = "TABLE sesblog (\n" +
            "  eventType string,\n" +
            "  event_date <type:int,logicalType:date,precision:10,scale:2>,\n" +
            "  col2 string,\n" +
            "  tags array<string>,\n" +
            "  col3 string) ";
    private static final String FILE_SUFFIX = ".parquet";
    private static int numberOfRecords;
    private ParquetOutputCodecConfig config;

    private static Record getRecord(int index) {
        List<HashMap> recordList = generateRecords(numberOfRecords);
        final Event event = JacksonLog.builder().withData(recordList.get(index)).build();
        return new Record<>(event);
    }

    private static List<HashMap> generateRecords(int numberOfRecords) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRecords; rows++) {

            HashMap<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", rows);
            eventData.put("doubleType", Double.valueOf(rows));
            eventData.put("floatType", Float.valueOf(rows));
            eventData.put("longType", Long.valueOf(rows));
            eventData.put("bytesType", ("Person"+rows).getBytes());
            recordList.add((eventData));

        }
        return recordList;
    }

    private static Schema parseSchema() {
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .name("doubleType").type().doubleType().noDefault()
                .name("floatType").type().floatType().noDefault()
                .name("longType").type().longType().noDefault()
                .name("bytesType").type().bytesType().noDefault()
                .endRecord();

    }

    private ParquetOutputCodec createObjectUnderTest() {
        config = new ParquetOutputCodecConfig();
        config.setSchema(parseSchema().toString());
        config.setBucket("test");
        config.setRegion("test");
        return new ParquetOutputCodec(config);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 10, 100})
    void test_happy_case(final int numberOfRecords) throws Exception {
        ParquetOutputCodecTest.numberOfRecords = numberOfRecords;
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final File tempFile = File.createTempFile(FILE_NAME, FILE_SUFFIX);
        parquetOutputCodec.start(tempFile);
        for (int index = 0; index < numberOfRecords; index++) {
            final Event event = (Event) getRecord(index).getData();
            parquetOutputCodec.writeEvent(event, outputStream, null);
        }
        parquetOutputCodec.closeWriter(outputStream, tempFile);
        List<HashMap<String, Object>> actualRecords = createParquetRecordsList(new ByteArrayInputStream(tempFile.toString().getBytes()));
        int index = 0;
        for (final HashMap<String, Object> actualMap : actualRecords) {
            assertThat(actualMap, notNullValue());
            Map expectedMap = generateRecords(numberOfRecords).get(index);
            assertThat(expectedMap, Matchers.equalTo(actualMap));
            index++;
        }
        tempFile.delete();
    }
    @Test
    public void test_getExtension() {
        ParquetOutputCodec parquetOutputCodec = createObjectUnderTest();
        String extension = parquetOutputCodec.getExtension();

        assertThat(extension, equalTo("parquet"));
    }

    @Test
    public void testSchemaGenerationFromTabular() throws IOException {
        Schema expectedSchema = new Schema.Parser().parse(expectedSchemaString);
        Schema actualSchema = ParquetSchemaParserFromTabularFormat.generateSchemaFromTabularString(inputString);
        assertThat(actualSchema, Matchers.equalTo(expectedSchema));
    }
    @Test
    public void whenNoSchemaProvided_thenThrowsException() {
        config = new ParquetOutputCodecConfig();
        config.setSchema(null);
        config.setFileLocation(null);
        config.setSchemaRegistryUrl(null);
        ParquetOutputCodec parquetOutputCodec = new ParquetOutputCodec(config);
        assertThat(parquetOutputCodec.checkS3SchemaValidity(), equalTo(Boolean.FALSE));
    }

    @Test
    public void test_s3SchemaValidity() {
        config = new ParquetOutputCodecConfig();
        config.setSchema(parseSchema().toString());
        config.setSchemaBucket("test");
        config.setSchemaRegion("test");
        config.setFileKey("test");
        ParquetOutputCodec parquetOutputCodec = new ParquetOutputCodec(config);
        assertThat(parquetOutputCodec.checkS3SchemaValidity(), equalTo(Boolean.TRUE));
        ParquetOutputCodec parquetOutputCodecFalse = createObjectUnderTest();
        assertThat(parquetOutputCodecFalse.checkS3SchemaValidity(), equalTo(Boolean.FALSE));
    }

    private List<HashMap<String, Object>> createParquetRecordsList(final InputStream inputStream) throws IOException {

        final File tempFile = File.createTempFile(FILE_NAME, FILE_SUFFIX);
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        List<HashMap<String, Object>> actualRecordList = new ArrayList<>();
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(tempFile.toURI()), new Configuration()), ParquetReadOptions.builder().build())) {
            final ParquetMetadata footer = parquetFileReader.getFooter();
            final MessageType schema = createdParquetSchema(footer);
            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                final long rows = pages.getRowCount();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int row = 0; row < rows; row++) {
                    final Map<String, Object> eventData = new HashMap<>();
                    int fieldIndex = 0;
                    final SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    for (Type field : schema.getFields()) {
                        try {
                            eventData.put(field.getName(), simpleGroup.getValueToString(fieldIndex, 0));
                        } catch (Exception parquetException) {
                            parquetException.printStackTrace();
                        }
                        fieldIndex++;
                    }
                    actualRecordList.add((HashMap) eventData);
                }
            }
        } catch (Exception parquetException) {
            parquetException.printStackTrace();
        } finally {
            Files.delete(tempFile.toPath());
        }
        return actualRecordList;
    }

    private MessageType createdParquetSchema(ParquetMetadata parquetMetadata) {
        return parquetMetadata.getFileMetaData().getSchema();
    }
}