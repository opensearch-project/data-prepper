package org.opensearch.dataprepper.plugins.sink.accumulator;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.NavigableSet;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mockito.Mock;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.sink.S3SinkConfig;
import org.opensearch.dataprepper.plugins.sink.configuration.BucketOptions;
import org.opensearch.dataprepper.plugins.sink.configuration.ObjectKeyOptions;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class BufferTypeTest {


    private S3Client s3Client;

    private S3SinkConfig s3SinkConfig;
    @Mock
    private BufferType bufferType;
    private String codecFileExtension = null;
    private static final String DEFAULT_CODEC_FILE_EXTENSION = "json";

    @BeforeEach
    public void setUp() {

        s3Client = S3Client.builder().region(Region.of("us-east-1")).build();
        String bucket = "dataprepper";
        s3SinkConfig = mock(S3SinkConfig.class);
        BucketOptions bucketOptions = mock(BucketOptions.class);
        ObjectKeyOptions objectKeyOptions = mock(ObjectKeyOptions.class);
        PluginModel pluginModel = mock(PluginModel.class);
        when(s3SinkConfig.getCodec()).thenReturn(pluginModel);

        codecFileExtension = s3SinkConfig.getCodec().getPluginName();
        if (codecFileExtension == null || codecFileExtension.isEmpty()) {
            codecFileExtension = DEFAULT_CODEC_FILE_EXTENSION;
        }
        when(s3SinkConfig.getBucketOptions()).thenReturn(bucketOptions);
        when(s3SinkConfig.getBucketOptions().getObjectKeyOptions()).thenReturn(objectKeyOptions);
        when(objectKeyOptions.getNamePattern()).thenReturn("my-elb-%{yyyy-MM-dd'T'hh-mm-ss}");
        when(s3SinkConfig.getBucketOptions().getBucketName()).thenReturn(bucket);
    }

    @Test
    void process_upload_to_s3_bucket() throws InterruptedException {
        BufferType bufferType = spy(BufferType.class);
        NavigableSet<String> bufferedEventSet = generateSet();
        StringBuilder eventBuilder = new StringBuilder();
        for (String event : bufferedEventSet) {
            eventBuilder.append(event);
        }
        Boolean uploadSuccess = bufferType.uploadToAmazonS3(s3SinkConfig,s3Client,RequestBody.fromString(eventBuilder.toString()));
        assertEquals(Boolean.TRUE,uploadSuccess);
    }

    @Test
    void start_should_throw_IllegalStateException_when_buffer_is_null()  {
        BufferType bufferType = spy(BufferType.class);

        assertThrows(NullPointerException.class, () ->  bufferType.uploadToAmazonS3(s3SinkConfig,s3Client,null));
    }

    private NavigableSet<String> generateSet() {
        DB eventDb = DBMaker.memoryDB().make();
        NavigableSet<String> bufferedEventSet = eventDb.treeSet("set").serializer(Serializer.STRING).createOrOpen();
        for (int i = 0; i < 5; i++) {
            final Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
            bufferedEventSet.add(event.toString());
        }
        return bufferedEventSet;
    }
}
