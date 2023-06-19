/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Implementation class for Download through Url
 */
public class HttpDBDownloadService implements DBSource {

    /**
     * HttpDBDownloadService constructor for initialisation of attributes
     */
    public HttpDBDownloadService( ) {
        //TODO
    }

    /**
     * Initialisation of Download through Url
     * @param urlList urlList
     */
    public void initiateDownload(List<DatabasePathURLConfig> urlList)  {
        //TODO
    }

    /**
     * Build Request And DownloadFile
     * @param url url
     */
    @Override
    public void buildRequestAndDownloadFile(String url)   {
        //TODO: Build Request And DownloadFile
    }

    /**
     * decompress And UnTar File
     * @param tarFolderPath tar Folder Path
     * @param tarFilepath tar File path
     * @param tmpDir tmp Dir
     */
    private static void decompressAndUnTarFile(String tarFolderPath, String tarFilepath, File tmpDir) {
       //TODO : decompress And UnTar File
    }

    /**
     * Delete Tar Folder
     * @param tarFolder Tar Folder
     */
    private static void deleteTarFolder(String tarFolder) {
        //TODO: Delete Tar Folder
    }

    /**
     * Download DB File From Maxmind
     * @param maxmindDownloadUrl maxmind Download Url
     * @param tarFilepath tar File path
     */
    private static void downloadDBFileFromMaxmind(String maxmindDownloadUrl, String tarFilepath) {
        //TODO: Download DB File From Maxmind
    }

    /**
     * DeCompress GZip File
     * @param gZippedFile  Zipped File
     * @param tarFile tar File
     * @return File
     * @throws IOException io exception
     */
    private static File deCompressGZipFile(File gZippedFile, File tarFile) throws IOException {
        //TODO: DeCompress GZip File
        return null;
    }

    /**
     * getFileName
     * @param inputFile input File
     * @param outputFolder output Folder
     * @return String
     */
    private static String getFileName(File inputFile, String outputFolder) {
        //TODO
        return null;
    }

    /**
     * unTarFile
     * @param tarFile tar File
     * @param destFile dest File
     * @throws IOException ioexception
     */
    private static void unTarFile(File tarFile, File destFile) throws IOException {
        //TODO
    }
}
