package org.apache.flink.streaming.examples.kvseparate.source;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * Random number source iterator.
 */
public class RandomNumberSourceIterator implements Iterator<Integer>, Serializable {

    private final int largest;
    private final Random rnd;

    public RandomNumberSourceIterator(int largest, long seed) {
        this.largest = largest;
        this.rnd = new Random(seed);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Integer next() {
        return rnd.nextInt(largest);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
