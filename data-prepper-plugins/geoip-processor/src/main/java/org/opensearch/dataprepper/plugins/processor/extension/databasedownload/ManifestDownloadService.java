/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.plugins.processor.exception.DownloadFailedException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader.MAXMIND_DATABASE_EXTENSION;


public class ManifestDownloadService implements DBSource {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int DEFAULT_BYTE_SIZE = 1024;
    private static final String CITY_DATABASE_NAME = "geolite2-city.mmdb";
    private static final String COUNTRY_DATABASE_NAME = "geolite2-country.mmdb";
    private static final String ASN_DATABASE_NAME = "geolite2-asn.mmdb";
    private static final String ZIP_FILE_EXTENSION = ".zip";
    private final String directoryName;

    private Manifest cityManifest;
    private Manifest countryManifest;
    private Manifest asnManifest;

    public ManifestDownloadService(final String directoryName) {
        this.directoryName = directoryName;
    }

    @Override
    public void initiateDownload(final List<String> databasePaths) {
        for(final String url : databasePaths) {
            final Manifest manifest = deserializeManifestFile(url);
            populateManifestObjects(manifest);

            final String manifestFilePath = manifest.getDbName();
            final String zipFileName = manifestFilePath.substring(0, manifestFilePath.lastIndexOf(".")).concat(ZIP_FILE_EXTENSION);
            final String zipFilePath = directoryName + File.separator + zipFileName;

            downloadZipFile(manifest.getUrl(), zipFilePath);
            unzipDownloadedFile(zipFilePath, directoryName);
        }
    }

    private Manifest deserializeManifestFile(final String CDNEndpoint) {
        HttpURLConnection httpURLConnection = null;
        try {
            final URL url = new URL(CDNEndpoint);
            httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.addRequestProperty("User-Agent", "Custom-User-Agent");

            final Manifest manifest = OBJECT_MAPPER.readValue(httpURLConnection.getInputStream(), Manifest.class);
            httpURLConnection.disconnect();

            return manifest;
        } catch (final IOException ex) {
            if (httpURLConnection != null) {
                httpURLConnection.disconnect();
            }
            throw new DownloadFailedException("Exception occurred while reading manifest.json file due to: " + ex);
        }
    }

    private void populateManifestObjects(final Manifest manifest) {
        switch (manifest.getDbName()) {
            case CITY_DATABASE_NAME:
                cityManifest = manifest;
                break;
            case COUNTRY_DATABASE_NAME:
                countryManifest = manifest;
                break;
            case ASN_DATABASE_NAME:
                asnManifest = manifest;
                break;
        }
    }

    private void downloadZipFile(final String databaseUrl, final String destinationPath) {
        HttpURLConnection httpURLConnection;
        try {
            final URL url = new URL(databaseUrl);
            httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.addRequestProperty("User-Agent", "Custom-User-Agent");
        } catch (IOException ex) {
            throw new DownloadFailedException("Exception occurred while opening connection due to: " + ex.getMessage());
        }

        try (final BufferedInputStream in = new BufferedInputStream(httpURLConnection.getInputStream());
             final FileOutputStream fileOutputStream = new FileOutputStream(destinationPath)) {
            final byte[] dataBuffer = new byte[DEFAULT_BYTE_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, DEFAULT_BYTE_SIZE)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
            httpURLConnection.disconnect();
        } catch (final IOException ex) {
            httpURLConnection.disconnect();
            throw new DownloadFailedException("Exception occurred while downloading MaxMind database due to: " + ex.getMessage());
        }
    }

    private void unzipDownloadedFile(final String zipFilePath, final String outputFilePath) {
        final File inputFile = new File(zipFilePath);
        final File outputDir = new File(outputFilePath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        final byte[] buffer = new byte[DEFAULT_BYTE_SIZE];

        try (final FileInputStream fileInputStream = new FileInputStream(inputFile);
             final ZipInputStream zipInputStream = new ZipInputStream(fileInputStream)) {

            ZipEntry zipEntry = zipInputStream.getNextEntry();
            while (zipEntry != null && zipEntry.getName().endsWith(MAXMIND_DATABASE_EXTENSION)) {
                final String fileName = zipEntry.getName();
                final File newFile = new File(outputDir + File.separator + fileName);

                final FileOutputStream fileOutputStream = new FileOutputStream(newFile);
                int len;
                while ((len = zipInputStream.read(buffer)) > 0) {
                    fileOutputStream.write(buffer, 0, len);
                }
                fileOutputStream.close();
                zipInputStream.closeEntry();
                zipEntry = zipInputStream.getNextEntry();
            }

            // deleting zip file after unzipping
            inputFile.delete();
        } catch (final IOException e) {
            inputFile.delete();
            throw new DownloadFailedException("Exception occurred while unzipping the database file due to: " + e.getMessage());
        }
    }


    public Manifest getCityManifest() {
        return cityManifest;
    }

    public Manifest getCountryManifest() {
        return countryManifest;
    }

    public Manifest getAsnManifest() {
        return asnManifest;
    }
}
