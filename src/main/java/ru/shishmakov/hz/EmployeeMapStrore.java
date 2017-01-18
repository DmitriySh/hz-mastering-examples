package ru.shishmakov.hz;

import com.hazelcast.core.MapStore;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter5_DistributedMap;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static ru.shishmakov.ch.Chapter5_DistributedMap.*;

/**
 * @author Dmitriy Shishmakov
 */
public class EmployeeMapStrore implements MapStore<String, Chapter5_DistributedMap.Employee2>, Serializable {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    @Override
    public void store(String key, Chapter5_DistributedMap.Employee2 value) {
        logger.debug("store key: {}", key);
    }

    @Override
    public void storeAll(Map<String, Chapter5_DistributedMap.Employee2> map) {
        logger.debug("store all");
        map.forEach(this::store);
    }

    @Override
    public void delete(String key) {
        logger.debug("delete key: {}", key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        logger.debug("delete all keys: {}", keys);
        keys.forEach(this::delete);
    }

    @Override
    public Chapter5_DistributedMap.Employee2 load(String key) {
        logger.debug("load key: {}", key);
        return new Chapter5_DistributedMap.Employee2(key, SHISHMAKOV);
    }

    @Override
    public Map<String, Chapter5_DistributedMap.Employee2> loadAll(Collection<String> keys) {
        logger.debug("loadAll keys: {}", keys);
        return keys.stream()
                .map(k -> Pair.of(k, new Employee2(k, SHISHMAKOV)))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Override
    public Iterable<String> loadAllKeys() {
        logger.debug("loadAllKeys");
        return Arrays.asList(DIMA, IGORE, SASHA, GALYA);
    }
}
