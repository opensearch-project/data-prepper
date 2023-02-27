package org.opensearch.dataprepper.plugins.parquetinputcodec;

import net.bytebuddy.utility.RandomString;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.record.Record;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class parquetInputCodecTest {

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private ParquetInputCodec parquetInputCodec;

    private ParquetInputCodec createObjectUnderTest() {
        return new ParquetInputCodec();
    }

    @BeforeEach
    void setup() {
        parquetInputCodec = createObjectUnderTest();
    }

    @Test
    void test_when_nullInputStream_then_throwsException() {
        assertThrows(NullPointerException.class, () ->
                parquetInputCodec.parse(null, eventConsumer));

        verifyNoInteractions(eventConsumer);
    }

    @Test
    void parse_with_null_Consumer_throws() throws IOException {
        parquetInputCodec = createObjectUnderTest();

        final InputStream inputStream = mock(InputStream.class);
        assertThrows(NullPointerException.class, () ->
                parquetInputCodec.parse(inputStream, null));
        verifyNoInteractions(inputStream);
    }

    @Test
    void parse_with_empty_InputStream_does_not_call_Consumer() throws IOException {
        final ByteArrayInputStream emptyInputStream = new ByteArrayInputStream(new byte[]{});
        createObjectUnderTest().parse(emptyInputStream, eventConsumer);
        verifyNoInteractions(eventConsumer);
    }

    @ParameterizedTest
    @ValueSource(ints = {10})
    void test_when_HappyCaseParquetInputStream_then_callsConsumerWithParsedEvents(final int numberOfRecords) throws IOException {
        InputStream inputStream = createRandomParquetStream(numberOfRecords);

        parquetInputCodec.parse(inputStream,eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(eventConsumer, times(numberOfRecords)).accept(recordArgumentCaptor.capture());
        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        assertThat(actualRecords.size(), equalTo(numberOfRecords));

        for (int i = 0; i < actualRecords.size(); i++) {

            final Record<Event> actualRecord = actualRecords.get(i);
            assertThat(actualRecord, notNullValue());
            assertThat(actualRecord.getData(), notNullValue());
            assertThat(actualRecord.getData().getMetadata(), notNullValue());
            assertThat(actualRecord.getData().getMetadata().getEventType(), equalTo(EventType.LOG.toString()));
        }

    }

    private static InputStream createRandomParquetStream(int numberOfRecords) throws IOException {

        Files.deleteIfExists(java.nio.file.Path.of("./test-parquet.parquet"));

        Schema schema = parseSchema();
        String OS = System.getProperty("os.name").toLowerCase();

        if (OS.contains("win")) {
            System.setProperty("hadoop.home.dir", Paths.get("").toAbsolutePath().toString());
        } else {
            System.setProperty("hadoop.home.dir", "/");
        }

        List<GenericData.Record> recordList = generateRecords(schema, numberOfRecords);
        java.nio.file.Path path = Paths.get("test-parquet.parquet");


        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(path.toUri()))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(new Configuration())
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build()) {

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }
            writer.close();

        }

        FileSystem fs = FileSystem.get(new Configuration());
        String filepath = path.toUri().getPath();
        InputStream parquetInputStream = new FileInputStream(path.toString());
        fs.delete(new Path(filepath),true);
        return parquetInputStream;

    }

    private static Schema parseSchema() {
        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\","
                + "\"type\": \"record\","
                + "\"name\": \"myrecordname\","
                + "\"fields\": ["
                + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]}"
                + ", {\"name\": \"myInteger\", \"type\": \"int\"}"
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    private static List<GenericData.Record> generateRecords(Schema schema, int numberOfRecords) {

        List<GenericData.Record> recordList = new ArrayList<>();

        for(int recordValue = 1; recordValue <= numberOfRecords; recordValue++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("myInteger", recordValue);
            record.put("myString", recordValue + RandomString.make());
            recordList.add(record);
        }
        return recordList;
    }


}
