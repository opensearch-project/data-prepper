/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class AvroSchemaParserFromSchemaRegistry {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG =  LoggerFactory.getLogger(AvroSchemaParserFromSchemaRegistry.class);
    static String getSchemaType(final String schemaRegistryUrl) {
        final StringBuilder response = new StringBuilder();
        String schemaType = "";
        try {
            final String urlPath = schemaRegistryUrl;
            final URL url = new URL(urlPath);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            final int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
                reader.close();
                final Object json = mapper.readValue(response.toString(), Object.class);
                final String indented = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                final JsonNode rootNode = mapper.readTree(indented);
                if(rootNode.get("schema") != null ){
                    return rootNode.get("schema").toString();
                }
            } else {
                final InputStream errorStream = connection.getErrorStream();
                final String errorMessage = readErrorMessage(errorStream);
                LOG.error("GET request failed while fetching the schema registry details : {}", errorMessage);
            }
        } catch (IOException e) {
            LOG.error("An error while fetching the schema registry details : ", e);
            throw new RuntimeException();
        }
        return null;
    }

    private static String readErrorMessage(final InputStream errorStream) throws IOException {
        if (errorStream == null) {
            return null;
        }
        final BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
        final StringBuilder errorMessage = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            errorMessage.append(line);
        }
        reader.close();
        errorStream.close();
        return errorMessage.toString();
    }
}
