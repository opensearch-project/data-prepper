package com.amazon.situp.processor.state;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Holds state for processors as a key/value mapping
 * @param <T>
 *     Type parameter for the value type. Keys will be Strings.
 */
public interface ProcessorState<T> {

    /**
     * Puts a key value pair in the processor state
     * @param key Key to put in the state
     * @param value Value to map to the key
     */
    void put(String key, T value);

    /**
     * Gets the value in the processor state for the given key
     * @param key Key to look up value for
     * @return Value for key, if it exists. Otherwise null.
     */
    T get(String key);

    /**
     * Gets all the data in the processor state
     * @return All the data in the processor state in the form of a Map
     */
    Map<String, T> getAll();

    /**
     * Iterate over the processor state with a bifunction
     * @param fn BiFunction with which to iterate over the processor state
     * @param <R> Type parameter for return value of BiFunction
     * @return Result of iteration as a list of objects of type R
     */
    public<R> List<R> iterate(BiFunction<String, T, R> fn);

    /**
     * Clears the data in the processor state
     */
    void clear();

    /**
     * Any cleanup code goes here
     */
    void close();
}