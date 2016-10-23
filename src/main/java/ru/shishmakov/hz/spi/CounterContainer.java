package ru.shishmakov.hz.spi;

import java.util.HashMap;
import java.util.Map;

/**
 * Hazelcast guarantees that within a single partition, only a single thread will be active.
 * So we don’t need to deal with concurrency control while accessing a container.
 *
 * @author Dmitriy Shishmakov on 23.10.16
 */
public class CounterContainer {
    private final Map<String, Integer> counters = new HashMap<>();

    public void init(String objectId) {
        counters.put(objectId, 0);
    }

    public void destroy(String objectId) {
        counters.remove(objectId);
    }

    public Integer increment(String objectId, int delta) {
        return counters.merge(objectId, delta, (a, b) -> a + b);
    }

    public Integer getCount(String objectId) {
        return counters.get(objectId);
    }

    public void clear() {
        counters.clear();
    }

    public void applyMigrationData(Map<String, Integer> migrationData) {
        counters.putAll(migrationData);
    }

    public Map<String, Integer> toMigrationData() {
        return new HashMap<>(counters);
    }
}
