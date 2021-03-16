package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class MapDBTraceIdTraceGroupCache {
    private static final String DB_PATH = "data/otel-trace-raw/";
    private static final String DB_NAME = "trace-id-trace-group-hashmap";

    private final HTreeMap<String, String> map;
    private final File dbFile;

    /**
     * This function creates the directory if it doesn't exists and returns the File.
     *
     * @param path
     * @return path
     * @throws RuntimeException if the directory can not be created.
     */
    private static File createPath(File path) {
        if (!path.exists()) {
            if (!path.mkdirs()) {
                throw new RuntimeException(String.format("Unable to create the directory at the provided path: %s", path.getName()));
            }
        }
        return path;
    }

    public MapDBTraceIdTraceGroupCache(final int concurrencyScale, final long ttl) {
        createPath(new File(DB_PATH));
        this.dbFile = new File(String.join("/", DB_PATH, DB_NAME));
        map = DBMaker.fileDB(dbFile)
                .fileDeleteAfterClose()
                .fileMmapEnable() //MapDB uses the (slower) Random Access Files by default
                .fileMmapPreclearDisable()
                .concurrencyScale(concurrencyScale)
                .make()
                .hashMap(DB_NAME)
                .expireAfterCreate(ttl)
                .counterEnable()
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.STRING)
                .createOrOpen();
    }

    public void put(final String key, final String value) {
        map.put(key, value);
    }

    public String get(final String key) {
        return map.get(key);
    }

    public String remove(final String key) {
        return map.remove(key);
    }

    public String compute(final String key, final BiFunction<String, String, String> remappingFunction) {
        return map.compute(key, remappingFunction);
    }

    public Set<Map.Entry<String, String>> entrySet() {
        return map.entrySet();
    }

    public long size() {
        return map.size();
    }

    public long sizeInBytes() {
        return dbFile.length();
    }

    public void delete() {
        map.close();
    }
}
