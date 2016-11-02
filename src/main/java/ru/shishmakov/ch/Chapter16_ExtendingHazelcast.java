package ru.shishmakov.ch;

import com.google.common.collect.Iterators;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.cfg.HzClusterConfig;
import ru.shishmakov.hz.spi.counter.*;

import java.lang.invoke.MethodHandles;
import java.util.*;
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
         * <li>Creates {@link HazelcastInstance} from 'hazelcast.xml'</li>
         * <li>Creates {@link CounterService} instance for each hz instance
             * <ul>
                 * <li>each instance invoke {@link CounterService#init(NodeEngine, Properties)}</li>
                 * <li>each instance contains array of {@link CounterContainer} with capacity 271 (partitions)</li>
             * </ul>
         * </li>
         * <li>Creates DO with custom name (objectId) {@link CounterService#createDistributedObject(String)}
             * <ul>
                *<li>finds partitionId by objectId (one of 271)</li>
                *<li>gets instance of {@link CounterContainer} by partitionId</li>
                *<li>each instance of {@link CounterContainer} contains inner field {@link Map}
                * where <u>key</u> is objectId and <u>value</u> is counter value</li>
                *<li>creates and return instance of {@link CounterProxy} that hold instances of {@link String} objectId,
                * {@link NodeEngine} nodeEngine, {@link CounterService} service</li>
             * </ul>
         * </li>
         * <li>Invokes {@link CounterProxy#increment(int)}
            * <ul>
                * <li>finds partitionId by objectId (one of 271)</li>
                * <li>creates instance of {@link IncOperation} to perform distributed increment on exact objectId</li>
                * <li>creates instance of {@link InvocationBuilder} to perform the operation on the cluster</li>
                * <li>instance of {@link InvocationBuilder} could create backup operation {@link IncBackupOperation}
                * to make aware operation {@link IncOperation}</li>
                * <li>waits response from {@link InvocationBuilder}</li>
            * </ul>
         * </li>
         * <li>Instance of {@link InvocationBuilder} invoke {@link IncOperation#run()}
            * <ul>
                * <li>gets instance of {@link CounterService} for that operation</li>
                * <li>gets instance of {@link CounterContainer} by partitionId</li>
                * <li>to increase the value for that objectId and save result value</li>
                * <li>invokes method {@link IncOperation#returnsResponse()} to define could the operation return result</li>
                * <li>invokes method {@link IncOperation#getResponse()}</li>
            * </ul>
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
