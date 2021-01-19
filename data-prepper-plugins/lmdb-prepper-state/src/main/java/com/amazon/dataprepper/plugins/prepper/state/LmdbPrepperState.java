package com.amazon.dataprepper.plugins.prepper.state;

import com.amazon.dataprepper.prepper.state.PrepperState;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LmdbPrepperState<T> implements PrepperState<byte[], T> {
    private static final Logger LOG = LoggerFactory.getLogger(LmdbPrepperState.class);
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Dbi<ByteBuffer> db;
    private final Env<ByteBuffer> env;
    private final Class<T> clazz; //Needed for deserialization
    private final File dbFile;


    /**
     * Constructor for LMDB prepper state. See LMDB-Java for more info:
     * https://github.com/lmdbjava/lmdbjava
     * @param dbFile The directory in which to store the LMDB data files
     * @param dbName Name of the database
     * @param clazz Class type for value storage
     */
    public LmdbPrepperState(final File dbFile, final String dbName, final Class<T> clazz) {
        //TODO: These need to be configurable
        env = Env.create()
                .setMapSize(10_485_760)
                .setMaxDbs(1)
                .setMaxReaders(10)
                .open(dbFile, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NOSUBDIR);
        db = env.openDbi(dbName, DbiFlags.MDB_CREATE);
        this.dbFile = dbFile;
        this.clazz = clazz;
    }

    private ByteBuffer toDirectByteBuffer(final byte[] in) {
        return ByteBuffer.allocateDirect(in.length).put(in).flip();
    }

    private T byteBufferToObject(final ByteBuffer valueBuffer) {
        try {
            final byte[] arr = new byte[valueBuffer.remaining()];
            valueBuffer.get(arr);
            valueBuffer.rewind();
            return OBJECT_MAPPER.readValue(arr, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void put(byte[] key, T value) {
        try {
            final ByteBuffer keyBuffer = toDirectByteBuffer(key);
            final ByteBuffer valueBuffer = toDirectByteBuffer(OBJECT_MAPPER.writeValueAsBytes(value));
            db.put(keyBuffer, valueBuffer);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void putAll(Map<byte[], T> batch) {
        try(Txn<ByteBuffer> txn = env.txnWrite()) {
            batch.entrySet().forEach(tEntry -> {
                try {
                    final ByteBuffer keyBuffer = toDirectByteBuffer(tEntry.getKey());
                    final ByteBuffer valueBuffer = toDirectByteBuffer(OBJECT_MAPPER.writeValueAsBytes(tEntry.getValue()));
                    db.put(txn, keyBuffer, valueBuffer);
                } catch (JsonProcessingException e) {
                    LOG.error("Caugh exception writing to db in putAll", e);
                }
            });
            txn.commit();
        }
    }

    //TODO: Test performance with single puts as above, and also with a putAll function which takes in a batch
    // of items to put into the lmdb

    @Override
    public T get(byte[] key) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer value = db.get(txn, toDirectByteBuffer(key));
            if (value == null) {
                return null;
            }
            return byteBufferToObject(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getBytes(ByteBuffer bb) {
        bb.rewind();
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        bb.rewind();
        return b;
    }

    @Override
    public Map<byte[], T> getAll() {
        try (final Txn<ByteBuffer> txn = env.txnRead()) {
            final Map<byte[], T> dbMap = new HashMap<>();
            db.iterate(txn).iterator().forEachRemaining(byteBufferKeyVal -> {
                dbMap.put(
                        getBytes(byteBufferKeyVal.key()),
                        byteBufferToObject(byteBufferKeyVal.val()));
            });

            return dbMap;
        }
    }

    @Override
    public<R> List<R> iterate(BiFunction<byte[], T, R> fn) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final List<R> returnVal = new ArrayList<>();
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn)) {
                final R val = fn.apply(getBytes(byteBufferKeyVal.key()),
                        byteBufferToObject(byteBufferKeyVal.val()));
                returnVal.add(val);
            }
            return returnVal;
        }
    }

    @Override
    public long size() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return db.stat(txn).entries;
        }
    }

    /**
     * TODO: Implement the sizeInBytes function
     * @return Size of the prepper state file, in bytes.
     */
    @Override
    public long sizeInBytes() {
        return 2097152;
    }

    private KeyRange getRange(int segments, int index) {
        final long sz = size();
        if(sz == 0) {
            return new KeyRange(0,-1);
        }
        long step = (long) Math.ceil(((double) sz / (double) segments));
        long lower = (long) index * step;
        long upper = Math.min(lower + step, sz);
        return new KeyRange(lower, upper);
    }

    /**
     * LMDB specific iterate function, which iterates over an index range using the LMDB cursor
     * @param fn Function to apply to elements
     * @param segments Number of segments
     * @param index index
     * @param <R> Result type
     * @return List of R objects representing the application of the function to the elements in the index range
     */
    @Override
    public<R> List<R> iterate(BiFunction<byte[], T, R> fn, final int segments, final int index) {
        final KeyRange keyRange = getRange(segments, index);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            Cursor<ByteBuffer> cursor = db.openCursor(txn);
            final List<R> returnVal = new ArrayList<>();
            cursor.first();
            //TODO: Look into faster way to move cursor up N elements
            for(long i=0; i<keyRange.start; i++) {
                cursor.next();
            }
            for(long i=keyRange.start; i<keyRange.end; i++) {
                final R val = fn.apply(getBytes(cursor.key()),
                        byteBufferToObject(cursor.val()));
                returnVal.add(val);
                if(!cursor.next()) {
                    break;
                }
            }
            cursor.close();
            return returnVal;
        }
    }

    @Override
    public void delete() {
        final File lockFile = new File(dbFile.getPath() + "-lock");
        if(!dbFile.delete()) {
            LOG.warn("Unable to delete database file at " + dbFile.getPath());
        }
        if(!lockFile.delete()) {
            LOG.warn("Unable to delete database file at " + lockFile.getPath());
        }
        env.close();
    }

    private static class KeyRange {
        public long start;
        public long end;

        public KeyRange(final long start, final long end) {
            this.start = start;
            this.end = end;
        }
    }
}
