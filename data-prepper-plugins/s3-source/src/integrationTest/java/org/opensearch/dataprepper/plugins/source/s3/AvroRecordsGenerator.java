package org.opensearch.dataprepper.plugins.source.s3;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.codec.avro.AvroInputCodec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class AvroRecordsGenerator implements RecordsGenerator {

    private static final String QUERY_STATEMENT ="select * from s3Object limit %d";

    private int numberOfRecords = 0;
    @Override
    public void write(File file, int numberOfRecords) throws IOException {
        this.numberOfRecords = numberOfRecords;

        Schema schema = parseSchema();

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter =new DataFileWriter<>(datumWriter);

        List<GenericRecord> recordList = generateRecords(schema);

        final OutputStream outputStream = new FileOutputStream(file);

        dataFileWriter.create(schema, outputStream);

        for(GenericRecord genericRecord: recordList) {
            dataFileWriter.append(genericRecord);
        }
        dataFileWriter.close();
    }

    @Override
    public InputCodec getCodec() {
        return new AvroInputCodec(TestEventFactory.getTestEventFactory());
    }

    @Override
    public String getFileExtension() {
        return "avro";
    }

    @Override
    public void assertEventIsCorrect(final Event event) {
        final String name = event.get("name", String.class);
        final Integer age = event.get("age", Integer.class);
        final Map<String, Object> innerRecord = (Map<String, Object>) event.get("nestedRecord", Object.class);
        final String firstFieldInNestedRecord = (String) innerRecord.get("firstFieldInNestedRecord");
        final Integer secondFieldInNestedRecord = (Integer) innerRecord.get("secondFieldInNestedRecord");

        assertThat(name, startsWith("Person"));
        assertThat(age, greaterThanOrEqualTo(0));
        assertThat(innerRecord, notNullValue());
        assertThat(firstFieldInNestedRecord, startsWith("testString"));
        assertThat(secondFieldInNestedRecord, greaterThanOrEqualTo(0));
    }

    @Override
    public String getS3SelectExpression() {
        return String.format(QUERY_STATEMENT, this.numberOfRecords);
    }

    @Override
    public boolean canCompress() {
        return false;
    }

    private List<GenericRecord> generateRecords(Schema schema) {

        List<GenericRecord> recordList = new ArrayList<>();

        for(int rows = 0; rows < numberOfRecords; rows++){

            GenericRecord record = new GenericData.Record(schema);
            GenericRecord innerRecord = new GenericData.Record(parseInnerSchemaForNestedRecord());
            innerRecord.put("firstFieldInNestedRecord", "testString"+rows);
            innerRecord.put("secondFieldInNestedRecord", rows);

            record.put("name", "Person"+rows);
            record.put("age", rows);
            record.put("nestedRecord", innerRecord);
            recordList.add((record));

        }

        return recordList;

    }

    private static Schema  parseSchema() {
        final Schema innerSchema = parseInnerSchemaForNestedRecord();
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .name("nestedRecord").type(innerSchema).noDefault()
                .endRecord();
    }

    private static Schema parseInnerSchemaForNestedRecord(){
        return SchemaBuilder
                .record("InnerRecord")
                .fields()
                .name("firstFieldInNestedRecord")
                .type(Schema.create(Schema.Type.STRING))
                .noDefault()
                .name("secondFieldInNestedRecord")
                .type(Schema.create(Schema.Type.INT))
                .noDefault()
                .endRecord();
    }
}
