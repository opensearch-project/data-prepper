/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Implementation class for Download through Url
 */
public class HttpDBDownloadService implements DBSource {
    private final String destinationDirectory;
    private static final int DEFAULT_BYTE_SIZE = 1024;
    private final GeoIPFileManager geoIPFileManager;
    private final MaxMindDatabaseConfig maxMindDatabaseConfig;

    /**
     * HttpDBDownloadService constructor for initialisation of attributes
     * @param destinationDirectory destinationDirectory
     */
    public HttpDBDownloadService(final String destinationDirectory,
                                 final GeoIPFileManager geoIPFileManager,
                                 final MaxMindDatabaseConfig maxMindDatabaseConfig) {
        this.destinationDirectory = destinationDirectory;
        this.geoIPFileManager = geoIPFileManager;
        this.maxMindDatabaseConfig = maxMindDatabaseConfig;
    }

    /**
     * Initialisation of Download through Url
     */
    public void initiateDownload() {
        final String tarDir = destinationDirectory + File.separator + "tar";
        final String downloadTarFilepath = tarDir + File.separator + "out.tar.gz";
        final Set<String> databasePaths = maxMindDatabaseConfig.getDatabasePaths().keySet();
        for (final String key: databasePaths) {
            geoIPFileManager.createDirectoryIfNotExist(tarDir);
            try {
                initiateSSL();
                buildRequestAndDownloadFile(maxMindDatabaseConfig.getDatabasePaths().get(key), downloadTarFilepath);
                final File tarFile = decompressAndgetTarFile(tarDir, downloadTarFilepath);
                unTarFile(tarFile, new File(destinationDirectory), key);
                deleteTarFolder(tarDir);
            } catch (Exception ex) {
                throw new DownloadFailedException("Failed to download from " + maxMindDatabaseConfig.getDatabasePaths().get(key)
                        + " due to: " + ex.getMessage());
            }
        }
    }

    /**
     * Decompress and untar the file
     * @param tarFolderPath tarFolderPath
     * @param downloadTarFilepath downloadTarFilepath
     *
     * @return File Tar file
     */
    private File decompressAndgetTarFile(final String tarFolderPath, final String downloadTarFilepath) {
        try {
            final File inputFile = new File(downloadTarFilepath);
            final String outputFile = getFileName(inputFile, tarFolderPath);
            File tarFile = new File(outputFile);
            // Decompress file
            return deCompressGZipFile(inputFile, tarFile);
        } catch (IOException ex) {
            throw new DownloadFailedException("Failed to decompress GZip file." + ex.getMessage());
        }
    }

    /**
     * Build Request And DownloadFile
     * @param url url
     */
    public void buildRequestAndDownloadFile(final String url, final String downloadTarFilepath)  {
        downloadDBFileFromMaxmind(url, downloadTarFilepath);
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
            throw new DownloadFailedException("Failed to download from " + maxmindDownloadUrl + " due to: " + ex.getMessage());
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
     * @param destDir dest directory
     * @param fileName File name
     *
     * @throws IOException ioexception
     */
    private static void unTarFile(final File tarFile, final File destDir, final String fileName) throws IOException {

        final FileInputStream fileInputStream = new FileInputStream(tarFile);
        final TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(fileInputStream);
        TarArchiveEntry tarEntry = null;

        while ((tarEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
            if(tarEntry.getName().endsWith(MAXMIND_DATABASE_EXTENSION)) {
                final File outputFile = new File(destDir + File.separator + fileName + MAXMIND_DATABASE_EXTENSION);

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
