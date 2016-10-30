package ru.shishmakov.ch;

import com.google.common.collect.Iterators;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.cfg.HzClusterConfig;
import ru.shishmakov.hz.spi.counter.Counter;
import ru.shishmakov.hz.spi.counter.CounterService;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static ru.shishmakov.hz.spi.counter.CounterService.CLASS_NAME;

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

        int hzCnt = 2;
        List<HazelcastInstance> hzs = IntStream.rangeClosed(1, hzCnt)
                .mapToObj(i -> getCustomHazelcastInstance())
                .collect(Collectors.toList());
        Iterator<HazelcastInstance> iterator = Iterators.cycle(hzs);
        logger.debug("Create {} hz cluster instances", hzs.size());

        int objectCnt = 4;
        int invokes = 5;
        List<Counter> counters = getCounterService(objectCnt, iterator::next);
        logger.debug("Get {} distributed instances of {} objects", counters.size(), CLASS_NAME);
        incrementCounter(counters, invokes);


        /*  ------------ add new cluster hz node --------------- */
        logger.debug("create new HZ instance and data should be migrate ");
        HazelcastInstance newHz = getCustomHazelcastInstance();
        sleep(3, SECONDS);

        /*  ------------ add new Counter DO --------------- */
        counters = getCounterService(objectCnt, () -> newHz);
        Counter newtCounter = newHz.getDistributedObject("CounterService", "cs" + (objectCnt + 1));
        counters.add(newtCounter);
        logger.debug("Get {} distributed instances of {} objects", counters.size(), CLASS_NAME);

        incrementCounter(counters, invokes);
        logger.debug("Finish increment {} : {}", counters.size(), CLASS_NAME);

        counters = getCounterService(objectCnt + 1, () -> newHz);
        printCounter(counters, invokes);

        sleep(3, SECONDS);
    }

    private static List<Counter> getCounterService(int doCnt, Supplier<HazelcastInstance> hz) {
        return IntStream.rangeClosed(1, doCnt)
                .mapToObj(i -> hz.get().<Counter>getDistributedObject("CounterService", "cs" + i))
                .collect(Collectors.toList());
    }

    private static void incrementCounter(Collection<Counter> counters, int invokes) {
        IntStream.rangeClosed(1, invokes).forEach(i -> {
            counters.forEach(c -> c.increment(1));
            logger.debug("--> {} step increment values: {} for each DO: {}", i, counters.size(), CLASS_NAME);
        });
        logger.debug("--------- :: + {} increments done :: ---------", invokes);
    }

    private static void printCounter(List<Counter> counters, int invokes) {
        IntStream.range(0, counters.size()).forEach(i ->
                logger.debug("<-- cs{} {} has result value: {} expected: {}", i + 1, CLASS_NAME, counters.get(i).get(), invokes * 2));
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
