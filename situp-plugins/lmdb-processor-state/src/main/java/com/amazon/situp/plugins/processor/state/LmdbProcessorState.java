package com.amazon.situp.plugins.processor.state;

import com.amazon.situp.processor.state.ProcessorState;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;
import org.lmdbjava.KeyRangeType;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

public class LmdbProcessorState<T> implements ProcessorState<T> {
    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Dbi<ByteBuffer> db;
    private final Env<ByteBuffer> env;
    private final Class<T> clazz; //Needed for deserialization

    /**
     * Constructor for LMDB processor state. See LMDB-Java for more info:
     * https://github.com/lmdbjava/lmdbjava
     * @param dbPath The directory in which to store the LMDB data files
     * @param dbName Name of the database
     * @param clazz Class type for value storage
     */
    public LmdbProcessorState(final File dbPath, final String dbName, final Class<T> clazz) {
        //TODO: These need to be configurable
        env = Env.create()
                .setMapSize(10_485_760)
                .setMaxDbs(1)
                .setMaxReaders(10)
                .open(dbPath, EnvFlags.MDB_NOTLS);
        db = env.openDbi(dbName, DbiFlags.MDB_CREATE);
        this.clazz = clazz;
    }

    private ByteBuffer toDirectByteBuffer(final byte[] in) {
        return ByteBuffer.allocateDirect(in.length).put(in).flip();
    }

    private T byteBufferToObject(final ByteBuffer valueBuffer) {
        try {
            final byte[] arr = new byte[valueBuffer.remaining()];
            valueBuffer.get(arr);
            return OBJECT_MAPPER.readValue(arr, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void put(String key, T value) {
        try {
            final ByteBuffer keyBuffer = toDirectByteBuffer(key.getBytes(StandardCharsets.UTF_8));
            final ByteBuffer valueBuffer = toDirectByteBuffer(OBJECT_MAPPER.writeValueAsBytes(value));
            db.put(keyBuffer, valueBuffer);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO: Test performance with single puts as above, and also with a putAll function which takes in a batch
    // of items to put into the lmdb

    @Override
    public T get(String key) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer value = db.get(txn, toDirectByteBuffer(key.getBytes(StandardCharsets.UTF_8)));
            if (value == null) {
                return null;
            }
            return byteBufferToObject(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, T> getAll() {
        try (final Txn<ByteBuffer> txn = env.txnRead()) {
            final Map<String, T> dbMap = new HashMap<>();
            db.iterate(txn).iterator().forEachRemaining(byteBufferKeyVal -> {
                dbMap.put(
                        StandardCharsets.UTF_8.decode(byteBufferKeyVal.key()).toString(),
                        byteBufferToObject(byteBufferKeyVal.val()));
            });

            return dbMap;
        }
    }


    @Override
    public void clear() {
        //TODO: we can delete and recreate a new DB
        try (final Txn<ByteBuffer> txn = env.txnWrite()) {
            db.drop(txn, true);
        }
    }

    @Override
    public<R> List<R> iterate(BiFunction<String, T, R> fn) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final List<R> returnVal = new ArrayList<>();
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn)) {
                final R val = fn.apply(StandardCharsets.UTF_8.decode(byteBufferKeyVal.key()).toString(),
                        byteBufferToObject(byteBufferKeyVal.val()));
                returnVal.add(val);
            }
            return returnVal;
        }
    }

    public<R> List<R> iterate(BiFunction<String, T, R> fn, final String starKeyRange, String endKeyRange) {
        final KeyRange<ByteBuffer> keyRange = new KeyRange<>(
                KeyRangeType.FORWARD_CLOSED,
                toDirectByteBuffer(starKeyRange.getBytes(StandardCharsets.UTF_8)),
                toDirectByteBuffer(endKeyRange.getBytes(StandardCharsets.UTF_8)));
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final List<R> returnVal = new ArrayList<>();
            for (CursorIterable.KeyVal<ByteBuffer> byteBufferKeyVal : db.iterate(txn, keyRange)) {
                final R val = fn.apply(StandardCharsets.UTF_8.decode(byteBufferKeyVal.key()).toString(),
                        byteBufferToObject(byteBufferKeyVal.val()));
                returnVal.add(val);
            }
            return returnVal;
        }
    }

    @Override
    public void close() {
        env.close();
    }
}
