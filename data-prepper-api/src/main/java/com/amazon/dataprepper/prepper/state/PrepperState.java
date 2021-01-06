package com.amazon.dataprepper.prepper.state;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Holds state for preppers as a key/value mapping
 * @param <T>
 *     Type parameter for the value type. Keys will be Strings.
 */
public interface PrepperState<K, V> {

    /**
     * Puts a key value pair in the prepper state
     * @param key Key to put in the state
     * @param value Value to map to the key
     */
    void put(K key, V value);

    /**
     * Gets the value in the prepper state for the given key
     * @param key Key to look up value for
     * @return Value for key, if it exists. Otherwise null.
     */
    V get(K key);

    /**
     * Gets all the data in the prepper state
     * @return All the data in the prepper state in the form of a Map
     */
    Map<K, V> getAll();

    /**
     * Iterate over the prepper state with a bifunction
     * @param fn BiFunction with which to iterate over the prepper state
     * @param <R> Type parameter for return value of BiFunction
     * @return Result of iteration as a list of objects of type R
     */
    public<R> List<R> iterate(BiFunction<K, V, R> fn);

    /**
     * Iterate over a segment of the prepper state with a bifunction
     * @param fn BiFunction with which to iterate over the prepper state
     * @param segments total number of segments
     * @param index segment index
     * @param <R> Type parameter for return value of BiFunction
     * @return Result of iteration as a list of objects of type R
     */
    public<R> List<R> iterate(BiFunction<K, V, R> fn, int segments, int index);

    /**
     * @return Size of the prepper state, in terms of number of elements stored.
     */
    public long size();

    /**
     * Any cleanup code goes here
     */
    void delete();
}