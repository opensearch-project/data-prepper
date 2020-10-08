package com.amazon.situp.plugins.processor.state;

import com.google.common.primitives.SignedBytes;
import com.amazon.situp.processor.state.ProcessorState;
import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.mapdb.BTreeMap;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerByteArray;

public class MapDbProcessorState<V> implements ProcessorState<byte[], V> {


    private static class SignedByteArraySerializer extends SerializerByteArray {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return SignedBytes.lexicographicalComparator().compare(o1, o2);
        }
    }

    private static final SignedByteArraySerializer SIGNED_BYTE_ARRAY_SERIALIZER = new SignedByteArraySerializer();

    private final BTreeMap<byte[], V> map;

    public MapDbProcessorState(final File dbPath, final String dbName) {
        map = (BTreeMap<byte[], V>) DBMaker.fileDB(String.join("/", dbPath.getPath(), dbName)).closeOnJvmShutdown().fileDeleteAfterClose().fileMmapEnable().fileMmapPreclearDisable().make()
                .treeMap(dbName).counterEnable().keySerializer(SIGNED_BYTE_ARRAY_SERIALIZER).valueSerializer(Serializer.JAVA).create();
    }


    @Override
    public void put(byte[] key, V value) {
        map.put(key, value);
    }

    public void putAll(final Map<byte[], V> data) {
        map.putAll(data);
    }

    @Override
    public V get(byte[] key) {
        return map.get(key);
    }

    @Override
    public Map<byte[], V> getAll() {
        return map;
    }

    @Override
    public <R> List<R> iterate(BiFunction<byte[], V, R> fn) {
        final List<R> returnList = new ArrayList<>();
        map.entryIterator().forEachRemaining(
                entry -> returnList.add(fn.apply(entry.getKey(), entry.getValue()))
        );
        return returnList;
    }

    //TODO: Make this function an interface method, because it can be used to split iteration without knowing about the underlying structure
    public <R> List<R> iterate(BiFunction<byte[], V, R> fn, final int segments, final int index) {
        if(map.size() == 0) {
            return Collections.EMPTY_LIST;
        }
        final List<byte[]> iterationEndpoints = getIterationEndpoints(segments, index);
        final List<R> returnList = new ArrayList<>();
        map.entryIterator(iterationEndpoints.get(0), true, iterationEndpoints.get(1), false).forEachRemaining(
                entry -> returnList.add(fn.apply(entry.getKey(), entry.getValue()))
        );
        return returnList;
    }

    private List<byte[]> getIterationEndpoints(final int segments, final int index) {
        final BigInteger lowEnd = new BigInteger(map.firstKey());
        final BigInteger highEnd = new BigInteger(map.lastKey());
        final BigInteger step = highEnd.subtract(lowEnd).divide(new BigInteger(String.valueOf(segments)));
        final byte[] lowIndex = lowEnd.add(step.multiply(new BigInteger(String.valueOf(index)))).toByteArray();
        final byte[] highIndex = index == segments - 1? highEnd.add(new BigInteger("1")).toByteArray() : lowEnd.add(step.multiply(new BigInteger(String.valueOf(index+1)))).toByteArray();
        final List<byte[]> iterationEndpoints = new ArrayList<>();
        iterationEndpoints.add(lowIndex);
        iterationEndpoints.add(highIndex);

        return iterationEndpoints;
    }

    private String bytesToString(final byte[] bytes) {
        String s = "[";
        for(int i=0; i<bytes.length; i++) {
            s += bytes[i] + " , ";
        }
        s += "]";
        return s;
    }


    @Override
    public long size() {
        return map.size();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public void close() {
        map.close();
    }
}
