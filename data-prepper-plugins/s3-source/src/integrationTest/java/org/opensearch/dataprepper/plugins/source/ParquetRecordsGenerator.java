package org.opensearch.dataprepper.plugins.source;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
public class ParquetRecordsGenerator implements RecordsGenerator{

    private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordsGenerator.class);
    public static final String EVENT_VERSION_FIELD = "eventVersion";
    public static final String PARQUET_CHECKSUM = ".crc";
    public static final String SERIAL_NUMBER_FIELD = "SlNo";
    public static final String EVENT_VERSION_VALUE = "1.0";
    public static final String QUERY_STATEMENT ="select  * from  S3Object s";
    public final String schemaJsonString = "{\"namespace\": \"org.opensearch\",\"type\": \"record\","
            + "\"name\": \"parquetRecords\",\"fields\": [ {\"name\": \"SlNo\", \"type\": \"int\"}"
            + ", {\"name\": \"eventVersion\", \"type\": \"string\"}]}";

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) throws IOException {
        final Path path = new Path(Instant.now().getEpochSecond()+"test.parquet");
        final java.nio.file.Path filePath = Paths.get(path.toString());
        try{
            Schema.Parser parser = new Schema.Parser().setValidate(true);
            final Schema schema = parser.parse(schemaJsonString);
            final List<GenericData.Record> recordList = generateParquetRecords(schema,numberOfRecords);
            OutputFile outputFile = HadoopOutputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(outputFile)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withValidation(false)
                    .withConf(new Configuration())
                    .withDictionaryEncoding(false)
                    .build()) {
                for (GenericData.Record parquetRecord : recordList) {
                    writer.write(parquetRecord);
                }
            }
            outputStream.write(Files.readAllBytes(filePath));
        } catch (Exception ex) {
            LOG.error("Error while writing parquet file.",ex);
        }finally {
            Files.deleteIfExists(filePath);
            Files.deleteIfExists(Paths.get("."+filePath+PARQUET_CHECKSUM));
        }
    }

    @Override
    public Codec getCodec() {
        return null;
    }

    @Override
    public String getFileExtension() {
        return "parquet";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {
        final Map<String, Object> messageMap = event.toMap();
        assertThat(messageMap, notNullValue());
        assertThat(messageMap.get(EVENT_VERSION_FIELD), equalTo(EVENT_VERSION_VALUE));
    }
    @Override
    public String getQueryStatement() {
        return QUERY_STATEMENT;
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    private static List<GenericData.Record> generateParquetRecords(Schema schema,int numberOfRecords) {
        List<GenericData.Record> recordDataList = new ArrayList<>();
        for(int i = 1; i <= numberOfRecords; i++) {
            GenericData.Record genericRecord = new GenericData.Record(schema);
            genericRecord.put(SERIAL_NUMBER_FIELD,i);
            genericRecord.put(EVENT_VERSION_FIELD, EVENT_VERSION_VALUE);
            recordDataList.add(genericRecord);
        }
        return recordDataList;
    }
}
