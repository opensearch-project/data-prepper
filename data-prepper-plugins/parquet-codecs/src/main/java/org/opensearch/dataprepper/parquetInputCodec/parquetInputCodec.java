package org.opensearch.dataprepper.parquetInputCodec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
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
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.function.Consumer;

@DataPrepperPlugin(name = "parquet", pluginType = InputCodec.class)
public class parquetInputCodec implements InputCodec {

    private static final String MESSAGE_FIELD_NAME = "message";

    private static final Logger LOG = LoggerFactory.getLogger(parquetInputCodec.class);

    @DataPrepperPluginConstructor
    public parquetInputCodec() {

    }

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        parseParquetStream(inputStream, eventConsumer);

    }

    private void parseParquetStream(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        // extracting parquet data in temporary file
        File tempFile = File.createTempFile("parquet-data", ".parquet");
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        // ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(tempFile.toURI())).build();

        ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(tempFile.toURI()), new Configuration()), ParquetReadOptions.builder().build());

        ParquetMetadata footer = parquetFileReader.getFooter();
        MessageType schema = createdParquetSchema(footer);
        List<Type> fields = schema.getFields();

//        for (Type field : fields) {
//            System.out.println(field.getName());
//            System.out.println(field.asPrimitiveType().getPrimitiveTypeName());
//        }

        List<SimpleGroup> simpleGroups = new ArrayList<>();

        PageReadStore pages;
        while ((pages = parquetFileReader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            // code to convert rows into events
            final Map<String, String> eventData = new HashMap<>();


            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                // converting it into eventData
                eventData.put(MESSAGE_FIELD_NAME, ((SimpleGroup) recordReader.read()).toString());

                // converting eventData into event and consumer accepting it
                final Event event = JacksonLog.builder().withData(eventData).build();

                eventConsumer.accept(new Record<>(event));

                simpleGroups.add(simpleGroup);
            }
        }
        parquetFileReader.close();

//        reader.close();
//        for(int i = 0; i < simpleGroups.size(); i++) {
//            System.out.println(simpleGroups.get(i));
//        }

    }

    private MessageType createdParquetSchema(ParquetMetadata parquetMetadata) {
        MessageType schema = parquetMetadata.getFileMetaData().getSchema();
        return schema;
    }

}
