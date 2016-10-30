package ru.shishmakov.hz.spi.counter;

import com.hazelcast.core.DistributedObject;

/**
 * @author Dmitriy Shishmakov on 19.10.16
 */
public interface Counter extends DistributedObject {
    int increment(int amount);

    int get();
}
