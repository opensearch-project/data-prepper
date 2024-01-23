/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
public class CsvCodecsIT {

    @Mock
    private CsvInputCodecConfig config;
    private CsvOutputCodecConfig outputConfig;

    @Mock
    private Consumer<Record<Event>> eventConsumer;

    private CsvInputCodec csvCodec;

    private CsvInputCodec createObjectUnderTest() {
        return new CsvInputCodec(config);
    }

    @BeforeEach
    void setup() {
        CsvInputCodecConfig defaultCsvCodecConfig = new CsvInputCodecConfig();
        lenient().when(config.getDelimiter()).thenReturn(defaultCsvCodecConfig.getDelimiter());
        lenient().when(config.getQuoteCharacter()).thenReturn(defaultCsvCodecConfig.getQuoteCharacter());
        lenient().when(config.getHeader()).thenReturn(defaultCsvCodecConfig.getHeader());
        lenient().when(config.isDetectHeader()).thenReturn(defaultCsvCodecConfig.isDetectHeader());

        csvCodec = createObjectUnderTest();
    }

    private CsvOutputCodec createOutputCodecObjectUnderTest() {
        outputConfig = new CsvOutputCodecConfig();
        outputConfig.setHeader(header());
        return new CsvOutputCodec(outputConfig);
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100, 200})
    void test_when_autoDetectHeaderHappyCase_then_callsConsumerWithParsedEvents(final int numberOfRows) throws IOException, CsvValidationException {
        final InputStream inputStream = createCsvInputStream(numberOfRows, header());
        CsvInputCodec csvInputCodec = createObjectUnderTest();
        csvInputCodec.parse(inputStream, eventConsumer);

        final ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);

        final List<Record<Event>> actualRecords = recordArgumentCaptor.getAllValues();
        CsvOutputCodec csvOutputCodec = createOutputCodecObjectUnderTest();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputCodecContext codecContext = new OutputCodecContext();
        csvOutputCodec.start(outputStream, null, codecContext);
        for (Record<Event> record : actualRecords) {
            csvOutputCodec.writeEvent(record.getData(), outputStream);
        }
        csvOutputCodec.complete(outputStream);
        //createTestFileFromStream(outputStream);
        String csvData = outputStream.toString(StandardCharsets.UTF_8);
        StringReader stringReader = new StringReader(csvData);
        CSVReader csvReader = new CSVReaderBuilder(stringReader).build();
        try {
            String[] line;
            int index = 0;
            int headerIndex;
            List<String> headerList = header();
            List<HashMap> expectedRecords = generateRecords(numberOfRows);
            while ((line = csvReader.readNext()) != null) {
                if (index == 0) {
                    headerIndex = 0;
                    for (String value : line) {
                        assertThat(headerList.get(headerIndex), Matchers.equalTo(value));
                        headerIndex++;
                    }
                } else {
                    headerIndex = 0;
                    for (String value : line) {
                        assertThat(expectedRecords.get(index - 1).get(headerList.get(headerIndex)), Matchers.equalTo(value));
                        headerIndex++;
                    }
                }
                index++;
            }
        } finally {
            csvReader.close();
            stringReader.close();
        }
    }


    private static List<HashMap> generateRecords(int numberOfRows) {

        List<HashMap> recordList = new ArrayList<>();

        for (int rows = 0; rows < numberOfRows; rows++) {

            HashMap<String, Object> eventData = new HashMap<>();

            eventData.put("name", "Person" + rows);
            eventData.put("age", Integer.toString(rows));
            recordList.add((eventData));

        }
        return recordList;
    }

    private static List<String> header() {
        List<String> header = new ArrayList<>();
        header.add("name");
        header.add("age");
        return header;
    }

    private InputStream createCsvInputStream(int numberOfRows, List<String> header) throws IOException {
        String csvData = createCsvData(numberOfRows, header);

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            OutputStreamWriter writer = new OutputStreamWriter(outputStream);
            writer.write(csvData);
            writer.flush();
            writer.close();
            byte[] bytes = outputStream.toByteArray();
            InputStream inputStream = new ByteArrayInputStream(bytes);
            return inputStream;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String createCsvData(int numberOfRows, List<String> header) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputStreamWriter writer = new OutputStreamWriter(outputStream);
        for (int i = 0; i < header.size(); i++) {
            writer.append(header.get(i));
            if (i != header.size() - 1) {
                writer.append(",");
            }
        }
        writer.append("\n");
        for (int i = 0; i < numberOfRows; i++) {
            writer.append("Person" + i);
            writer.append(",");
            writer.append(Integer.toString(i));
            writer.append("\n");
        }
        writer.flush();
        writer.close();
        outputStream.close();
        return outputStream.toString();
    }
}
