package org.opensearch.dataprepper.parquetInputCodec;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.awt.*;
import java.io.InputStream;
import java.util.function.Consumer;

@ExtendWith(MockitoExtension.class)
public class parquetInputCodecTest {

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    static InputStream createParquetRandomStream(int numberOfColumns, int numberOfRecords) {

        MessageType schema = MessageTypeParser.parseMessageType("message schema { required int32 column1; required double column2; }");

        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        //ParquetWriter<Group> writer = new ParquetWriter<>();
        //

        return null;
    }



}
