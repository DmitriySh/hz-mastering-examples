package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.cfg.HzClusterConfig;

import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov on 17.10.16
 */
public class Chapter16_ExtendingHazelcast {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples() {
        logger.info("-- Chapter 16. Extending Hazelcast --");

        useSPICreateCustomDistributedCounter();
    }

    private static void useSPICreateCustomDistributedCounter() {
        logger.debug("-- Service Provider Interface custom distributed counter --");

        HazelcastInstance hzCh16 = HzClusterConfig.buildFromFileDirectly("src/main/resources/hazelcast-ch16.xml");
//        DistributedObject object = hzCh16.getDistributedObject("CounterService", "cs1");
        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }

}
