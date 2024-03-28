package org.opensearch.dataprepper.plugins.source.s3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetInputCodec;
import org.opensearch.dataprepper.plugins.fs.LocalOutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ParquetRecordsGenerator implements RecordsGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordsGenerator.class);

    private static final String SCHEMA_JSON =
            "{\"namespace\": \"org.example.test\"," +
                    " \"type\": \"record\"," +
                    " \"name\": \"TestMessage\"," +
                    " \"fields\": [" +
                    "     {\"name\": \"id\", \"type\": \"string\"}," +
                    "     {\"name\": \"value\", \"type\": \"int\"}," +
                    "     {\"name\": \"alternateIds\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}," +
                    "     {\"name\": \"lastUpdated\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}" +
                    " ]}";

    private static final String QUERY_STATEMENT ="select * from s3Object limit %d";

    private int numberOfRecords = 0;

    @Override
    public void write(final File file, final int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;
        try {
            Schema schema = new Schema.Parser().parse(SCHEMA_JSON);

            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
                    .withSchema(schema)
                    .build();

            for (int i = 0; i < numberOfRecords; i++) {
                GenericData.Record record = new GenericData.Record(schema);
                record.put("id", "id");
                record.put("value", i);
                record.put("alternateIds", Arrays.asList("altid1", "altid2"));
                record.put("lastUpdated", 1684509331977L);

                writer.write(record);
            }
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputCodec getCodec() {
        return new ParquetInputCodec(TestEventFactory.getTestEventFactory());
    }

    @Override
    public String getFileExtension() {
        return "parquet";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {
        final String id = event.get("id", String.class);
        final int value = event.get("value", Integer.class);
        final List<String> alternateIds = (List<String>) event.get("alternateIds", List.class);
        final long lastUpdated = event.get("lastUpdated", Long.class);

        assertThat(id, equalTo("id"));
        assertThat(value, greaterThanOrEqualTo(0));
        assertThat(alternateIds, containsInAnyOrder("altid1", "altid2"));
        assertThat(lastUpdated, equalTo(1684509331977L));
    }
    @Override
    public String getS3SelectExpression() {
        return String.format(QUERY_STATEMENT, this.numberOfRecords);
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean canCompress() {
        return false;
    }
}
