package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.util.UuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter2 {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static void doExampels(HazelcastInstance hz1, HazelcastInstance hz2) {
        IMap<String, Number> map1 = hz1.getMap("testmap1");
        IMap<String, Number> map2Node1 = hz1.getMap("testmap2");
        IMap<String, Number> map2Node2 = hz2.getMap("testmap2");

        IdGenerator generator = hz1.getIdGenerator("IdGenerator");
        IMap<String, Number> map3 = hz1.getMap("testmap" + generator.newId());
        String key = UuidUtil.createClusterUuid();
        IMap<String, Number> map4 = hz2.getMap("testmap3@" + key);
        IMap<String, Number> map5 = hz2.getMap("testmap4s@" + key);

        map2Node1.set("Key", 1L);
        logger.debug("Before node destroy: map2Node1: {}, map2Node2: {}", map2Node1.size(), map2Node2.size());
        map2Node2.destroy();
        logger.debug("After node destroy: map2Node1: {}, map2Node2: {}", map2Node1.size(), map2Node2.size());
    }
}
