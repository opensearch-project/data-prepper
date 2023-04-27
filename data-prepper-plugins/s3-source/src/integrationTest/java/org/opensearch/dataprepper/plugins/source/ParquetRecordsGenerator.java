package org.opensearch.dataprepper.plugins.source;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetRecordsGenerator implements RecordsGenerator{
    private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordsGenerator.class);

    private static final String QUERY_STATEMENT ="select count(*) as total from  S3Object s";

    private static final String PARQUET_FILE_RELATIVE_PATH = "\\src\\main\\resources\\IntegrationTest.parquet";

    @Override
    public void write(final int numberOfRecords, final OutputStream outputStream) throws IOException {
        final byte[] bytes = Files.readAllBytes(Path.of(Paths.get("").toAbsolutePath().toString() + PARQUET_FILE_RELATIVE_PATH));
        outputStream.write(bytes);
    }

    @Override
    public InputCodec getCodec() {
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
        assertThat(messageMap.get("Year"),equalTo("2018"));
        assertThat(messageMap.get("count"),nullValue());
        assertTrue(Integer.valueOf(String.valueOf(messageMap.get("Sex")))>=1);
        assertTrue(Integer.valueOf(String.valueOf(messageMap.get("Ethnic")))>=1);
        assertTrue(Integer.valueOf(String.valueOf(messageMap.get("Area")))>=1);
    }
    @Override
    public String getS3SelectExpression() {
        return QUERY_STATEMENT;
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
