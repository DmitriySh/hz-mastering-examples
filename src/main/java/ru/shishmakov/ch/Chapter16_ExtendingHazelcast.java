package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.cfg.HzClusterConfig;
import ru.shishmakov.hz.spi.counter.Counter;
import ru.shishmakov.hz.spi.counter.CounterService;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static ru.shishmakov.hz.spi.counter.CounterService.NAME;

/**
 * @author Dmitriy Shishmakov on 17.10.16
 */
public class Chapter16_ExtendingHazelcast {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples() {
        logger.info("-- Chapter 16. Extending Hazelcast --");

        createCustomDistributedCounterWithSPI();
    }

    /**
     * Step by step:<br/>
     * <ol>
     * <li>Create {@link HazelcastInstance} from 'hazelcast-ch16.xml'</li>
     * <li>Create {@link CounterService} instance for each hz instance
     * <ul>
     * <li>Ea</li>
     * <li>2</li>
     * </ul>
     * </li>
     * </li>
     * </ol>
     */
    private static void createCustomDistributedCounterWithSPI() {
        logger.debug("-- Service Provider Interface custom distributed counter --");

        int count = 2;
        List<HazelcastInstance> hzs = IntStream.range(0, count)
                .mapToObj(i -> getCustomHazelcastInstance())
                .collect(Collectors.toList());
        logger.debug("Create {} hz cluster instances", hzs.size());

        List<Counter> counters = IntStream.range(0, count)
                .mapToObj(i -> hzs.get(i).<Counter>getDistributedObject("CounterService", "cs" + i))
                .collect(Collectors.toList());
        logger.debug("Get {} distributed instances of {} objects", counters.size(), NAME);
        incrementCounters(counters, 5);


        /*  ------------ new cluster node --------------- */
        logger.debug("create new HZ instance and data should be migrate ");
        HazelcastInstance hz = getCustomHazelcastInstance();
        sleep(3, SECONDS);

        counters = IntStream.range(0, count + 1)
                .mapToObj(i -> hz.<Counter>getDistributedObject("CounterService", "cs" + i))
                .collect(Collectors.toList());
        logger.debug("Get {} distributed instances of {} objects", counters.size(), NAME);
        incrementCounters(counters, 5);

        logger.debug("Finish increment", counters.size(), NAME);
        sleep(3, SECONDS);
    }

    private static void incrementCounters(List<Counter> counters, int invokes) {
        for (int i = 1; i <= invokes; i++) {
            counters.forEach(c -> c.increment(1));
            logger.debug("{} step increment values for each DO", i, counters.size(), NAME);
        }
    }

    private static void sleep(int sleep, TimeUnit unit) {
        try {
            Thread.sleep(unit.toMillis(sleep));
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }

    private static HazelcastInstance getCustomHazelcastInstance() {
        return HzClusterConfig.buildFromFileDirectly("src/main/resources/hazelcast-ch16.xml");
    }

}
