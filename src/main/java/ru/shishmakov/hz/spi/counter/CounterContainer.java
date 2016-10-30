package ru.shishmakov.hz.spi.counter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for every partition in the system. It will contain all counters and proxies for a given partition.
 * <p/>
 * Hazelcast guarantees that within a single partition, only a single thread will be active.
 * So we don’t need to deal with concurrency control while accessing a container.
 *
 * @author Dmitriy Shishmakov on 23.10.16
 */
public class CounterContainer {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String CLASS_NAME = CounterContainer.class.getSimpleName();

    private final Map<String, Integer> counters = new HashMap<>();

    public void init(String objectId) {
        counters.put(objectId, 0);
    }

    public void destroy(String objectId) {
        counters.remove(objectId);
        logger.debug("{} remove key: {}", CLASS_NAME, objectId);
    }

    public Integer increment(String objectId, int delta) {
        return counters.merge(objectId, delta, (a, b) -> a + b);
    }

    public Integer getCount(String objectId) {
        return counters.get(objectId);
    }

    /**
     * This method is called for two reasons:<br/>
     * - the partition migration has succeeded and the old partition owner can get rid of all the data in partition<br/>
     * - partition migration operation fails and the new partition owner needs to roll back its changes
     */
    public void clear() {
        counters.clear();
    }

    /**
     * This method is called on the member of new partition owner
     */
    public void applyMigrationData(Map<String, Integer> migrationData) {
        counters.putAll(migrationData);
    }

    /**
     * This method is called when Hazelcast wants to start the migration of the partition
     * on the member that currently owns the partition
     */
    public Map<String, Integer> toMigrationData() {
        return new HashMap<>(counters);
    }
}
