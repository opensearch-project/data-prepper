package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Avrotest {
    public static void main(String[] args) throws IOException {
        Schema schema = parseSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream outputStream=new ByteArrayOutputStream();

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, outputStream);
        System.out.println("EXECUTED!");
        for(GenericRecord record: generateRecords(schema,100)){
            dataFileWriter.append(record);
        }
        dataFileWriter.close();
        final byte[] avroData=outputStream.toByteArray();
        ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
        Schema schema1=stream.getSchema();

        while (stream.hasNext()) {

            GenericRecord avroRecord= stream.next();

            for(Schema.Field field: avroRecord.getSchema().getFields()){
                System.out.print(" Key: "+ field.name()+" Value: "+avroRecord.get(field.name()));
            }
            System.out.println();
        }


    }
    private static Schema parseSchema() {
        return SchemaBuilder.record("Person")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().intType().noDefault()
                .endRecord();

    }

    private static List<GenericRecord> generateRecords(Schema schema, int numberOfRecords) {

        List<GenericRecord> recordList = new ArrayList<>();

        for(int rows = 0; rows < numberOfRecords; rows++){

            GenericRecord record = new GenericData.Record(schema);

            record.put("name", "Person"+rows);
            record.put("age", rows);
            recordList.add((record));

        }

        return recordList;

    }
}
