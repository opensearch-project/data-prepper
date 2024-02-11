/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.net.URL;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Implementation class for Download through Url
 */
public class HttpDBDownloadService implements DBSource {

    private static final Logger LOG = LoggerFactory.getLogger(HttpDBDownloadService.class);
    private final String prefixDir;
    private static final int DEFAULT_BYTE_SIZE = 1024;
    private final GeoIPFileManager geoIPFileManager;

    /**
     * HttpDBDownloadService constructor for initialisation of attributes
     * @param prefixDir prefixDir
     */
    public HttpDBDownloadService(final String prefixDir, final GeoIPFileManager geoIPFileManager) {
        this.prefixDir = prefixDir;
        this.geoIPFileManager = geoIPFileManager;
    }

    /**
     * Initialisation of Download through Url
     * @param urlList urlList
     */
    public void initiateDownload(List<String> urlList) {
        final File tmpDir = DBSource.createFolderIfNotExist(tempFolderPath + File.separator + prefixDir);
        for(String url : urlList) {
            DBSource.createFolderIfNotExist(tarFolderPath);
            try {
                initiateSSL();
                buildRequestAndDownloadFile(url);
                decompressAndUntarFile(tarFolderPath, downloadTarFilepath, tmpDir);
                deleteTarFolder(tarFolderPath);
            } catch (Exception ex) {
                LOG.info("InitiateDownload Exception {0} " , ex);
            }
        }
    }

    /**
     * Decompress and untar the file
     * @param tarFolderPath tarFolderPath
     * @param downloadTarFilepath downloadTarFilepath
     * @param tmpDir tmpDir
     */
    private void decompressAndUntarFile(String tarFolderPath, String downloadTarFilepath, File tmpDir) {
        try {
            final File inputFile = new File(downloadTarFilepath);
            final String outputFile = getFileName(inputFile, tarFolderPath);
            File tarFile = new File(outputFile);
            // Decompress file
            tarFile = deCompressGZipFile(inputFile, tarFile);
            // Untar file
            unTarFile(tarFile, tmpDir);
        } catch (IOException ex) {
            LOG.info("Decompress and untar the file Exception {0} " , ex);
        }
    }

    /**
     * Build Request And DownloadFile
     * @param url url
     */
    public void buildRequestAndDownloadFile(String... url)  {
        downloadDBFileFromMaxmind(url[0], downloadTarFilepath);
    }

    /**
     * Delete Tar Folder
     * @param tarFolder Tar Folder
     */
    private void deleteTarFolder(String tarFolder) {
        final File file = new File(tarFolder);
        geoIPFileManager.deleteDirectory(file);
        if (file.exists()) {
            file.delete();
        }
    }

    /**
     * Download DB File From Maxmind
     * @param maxmindDownloadUrl maxmind Download Url
     * @param tarFilepath tar File path
     */
    private static void downloadDBFileFromMaxmind(String maxmindDownloadUrl, String tarFilepath) {
        try (final BufferedInputStream in = new BufferedInputStream(new URL(maxmindDownloadUrl).openStream());
             final FileOutputStream fileOutputStream = new FileOutputStream(tarFilepath)) {
            final byte[] dataBuffer = new byte[DEFAULT_BYTE_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(dataBuffer, 0, DEFAULT_BYTE_SIZE)) != -1) {
                fileOutputStream.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException ex) {
            LOG.info("download DB File FromMaxmind Exception {0} " , ex);
        }
    }

    /**
     * DeCompress GZip File
     * @param gZippedFile  Zipped File
     * @param tarFile tar File
     * @return File
     * @throws IOException io exception
     */
    private static File deCompressGZipFile(File gZippedFile, File tarFile) throws IOException {

        final FileInputStream fileInputStream = new FileInputStream(gZippedFile);
        final GZIPInputStream gZIPInputStream = new GZIPInputStream(fileInputStream);

        final FileOutputStream fileOutputStream = new FileOutputStream(tarFile);
        final byte[] buffer = new byte[DEFAULT_BYTE_SIZE];
        int len;
        while ((len = gZIPInputStream.read(buffer)) > 0) {
            fileOutputStream.write(buffer, 0, len);
        }
        fileOutputStream.close();
        gZIPInputStream.close();
        return tarFile;
    }

    /**
     * getFileName
     * @param inputFile input File
     * @param outputFolder output Folder
     * @return String
     */
    private static String getFileName(File inputFile, String outputFolder) {
        return outputFolder + File.separator +
                inputFile.getName().substring(0, inputFile.getName().lastIndexOf('.'));
    }

    /**
     * unTarFile
     * @param tarFile tar File
     * @param destFile dest File
     * @throws IOException ioexception
     */
    private static void unTarFile(File tarFile, File destFile) throws IOException {

        final FileInputStream fileInputStream = new FileInputStream(tarFile);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(fileInputStream);
        TarArchiveEntry tarEntry = null;

        while ((tarEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
            if(tarEntry.getName().endsWith(".mmdb")) {
                String fileName = destFile + File.separator + tarEntry.getName().split("/")[1];
                final File outputFile = new File(fileName);
                if (tarEntry.isDirectory()) {
                    if (!outputFile.exists()) {
                        outputFile.mkdirs();
                    }
                } else {
                    outputFile.getParentFile().mkdirs();
                    final FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
                    IOUtils.copy(tarArchiveInputStream, fileOutputStream);
                    fileOutputStream.close();
                }
            }
        }
        tarArchiveInputStream.close();
    }
}
