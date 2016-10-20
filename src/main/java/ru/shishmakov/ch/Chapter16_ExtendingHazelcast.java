package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.cfg.HzClusterConfig;
import ru.shishmakov.hz.spi.Counter;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ru.shishmakov.hz.spi.CounterService.NAME;

/**
 * @author Dmitriy Shishmakov on 17.10.16
 */
public class Chapter16_ExtendingHazelcast {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples() {
        logger.info("-- Chapter 16. Extending Hazelcast --");

        createCustomDistributedCounterWithSPI();
    }

    private static void createCustomDistributedCounterWithSPI() {
        logger.debug("-- Service Provider Interface custom distributed counter --");

        int count = 3;
        List<HazelcastInstance> hzs = IntStream.range(0, count)
                .mapToObj(i -> HzClusterConfig.buildFromFileDirectly("src/main/resources/hazelcast-ch16.xml"))
                .collect(Collectors.toList());
        logger.debug("Create {} hz cluster instances", hzs.size());

        List<Counter> counters = IntStream.range(0, count)
                .mapToObj(i -> hzs.get(i).<Counter>getDistributedObject("CounterService", "cs" + i))
                .collect(Collectors.toList());
        logger.debug("Create {} instances of {} objects", counters.size(), NAME);

        counters.forEach(c -> c.increment(1));
        logger.debug("Increment values for each DO", counters.size(), NAME);

        try {
            Thread.sleep(3_000);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }

}
