package ru.shishmakov.hz;

import com.hazelcast.core.MapStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter5;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static ru.shishmakov.ch.Chapter5.*;

/**
 * @author Dmitriy Shishmakov
 */
public class EmployeeMapStrore implements MapStore<String, Chapter5.Employee2>, Serializable {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());



    @Override
    public void store(String key, Chapter5.Employee2 value) {
        logger.debug("store key: {}", key);
    }

    @Override
    public void storeAll(Map<String, Chapter5.Employee2> map) {
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
    public Chapter5.Employee2 load(String key) {
        logger.debug("load key: {}", key);
        return new Chapter5.Employee2(key, SHISHMAKOV);
    }

    @Override
    public Map<String, Chapter5.Employee2> loadAll(Collection<String> keys) {
        logger.debug("loadAll keys: {}", keys);
        Map<String, Employee2> map = new HashMap<>();
        keys.forEach(k -> map.put(k, new Employee2(k, SHISHMAKOV)));
        return map;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        logger.debug("loadAllKeys");
        return Arrays.asList(DIMA, IGORE, SASHA, GALYA);
    }
}
