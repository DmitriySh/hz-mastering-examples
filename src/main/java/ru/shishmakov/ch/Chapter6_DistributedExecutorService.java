package ru.shishmakov.ch;

import com.hazelcast.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;


/**
 * @author Dmitriy Shishmakov
 */
public class Chapter6_DistributedExecutorService {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    public static void doExamples(ExecutorService service) {
        logger.debug("-- Chapter 6. Distributed Executor Service --");

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

//        jucSimpleExecutor(service);
//        hzSimpleExecutor(hz1, hz2);
//        executorOnMembers(hz1, hz2);
//        executorOnKeyOwner(hz1, hz2);
//        executeGetFuture(hz1, hz2);
        executeGetFutureCallback(hz1, hz2);
    }

    private static void executeGetFutureCallback(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IExecutorService Future with callback--");

        int number = 10;
        IExecutorService service = hz1.getExecutorService("executor");
        service.submit(new FibonacciTask(number), new FibExecutionCallback(number));

        endExecutor(service);
        service.destroy();
    }

    private static void executeGetFuture(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IExecutorService get Future --");

        int number = 10;
        IExecutorService service = hz1.getExecutorService("executor");
        Future<Long> future = service.submit(new FibonacciTask(number));

        try {
            logger.info("Fibonacci {} is result = {}", number, future.get(1, TimeUnit.MINUTES));
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Error", e);
        }

        endExecutor(service);
        service.destroy();
    }

    private static void executorOnKeyOwner(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IExecutorService on key owner --");

        buildHZInstance();
        IMap<UUID, Object> map = hz1.getMap("map");
        IExecutorService service = hz2.getExecutorService("executor");
        for (int step = 5; step > 0; step--) {
            UUID key = UUID.randomUUID();
            map.put(key, step);
            logger.debug("put key: {}, value: {}", key, step);
        }
        for (UUID key : map.keySet()) {
            service.executeOnKeyOwner(new KeyOwnerTask(key, "map"), key);
        }

//        for (UUID key : map.keySet()) {
//            service.execute(new KeyOwnerTask(key, "map"));
//        }

        endExecutor(service);
        map.destroy();
    }

    private static void executorOnMembers(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IExecutorService on members --");

        buildHZInstance();
        IExecutorService service = hz2.getExecutorService("executor");
        for (Member member : hz1.getCluster().getMembers()) {
            service.executeOnMember(new HeartBeatTask(1), member);
            logger.debug("Start heartbeat task async on member: {}, uuid: {}", member.getAddress(), member.getUuid());
        }

        service.executeOnAllMembers(new HeartBeatTask(2));
        logger.debug("Start heartbeat task async on each member");

        endExecutor(service);
    }

    private static void hzSimpleExecutor(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Simple IExecutorService --");

        IExecutorService service = hz1.getExecutorService("defaultEx");
        for (int step = 3; step > 0; step--) {
            service.execute(new HeartBeatTask(step));
            logger.debug("Start heartbeat task in Hz IExService async");
        }

        endExecutor(service);
    }

    private static void jucSimpleExecutor(ExecutorService service) {
        logger.debug("-- JUC Simple ExecutorService --");

        for (int step = 3; step > 0; step--) {
            service.execute(new HeartBeatTask(step));
            logger.debug("Start heartbeat task in JUC ExService async");
        }
        try {
            service.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }

    public static class FibExecutionCallback implements ExecutionCallback<Long> {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
        private final long number;

        public FibExecutionCallback(long number) {
            this.number = number;
        }

        @Override
        public void onResponse(Long response) {
            logger.info("Fibonacci {} is result = {}", number, response);
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("Error", t);
        }
    }


    public static class FibonacciTask implements Callable<Long>, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
        private final long number;

        public FibonacciTask(long number) {
            this.number = number;
        }

        @Override
        public Long call() throws Exception {
            return calculate(number);
        }

        private long calculate(long number) {
            if (number <= 1) return number;
            long n1 = calculate(number - 1);
            long n2 = calculate(number - 2);
            logger.debug("{} + {}", n1, n2);
            return n1 + n2;
        }
    }

    public static class KeyOwnerTask implements Runnable, Serializable, HazelcastInstanceAware {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private transient HazelcastInstance hz;
        private final UUID key;
        private final String mapName;

        public KeyOwnerTask(UUID key, String mapName) {
            this.key = key;
            this.mapName = mapName;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            IMap<UUID, Object> map = hz.getMap(mapName);
            Set<UUID> keySet = map.localKeySet();
            logger.debug("task key: {} value: {} [local: {}] localKeySet: {}",
                    key, map.get(key), keySet.contains(key), keySet);
        }
    }

    public static class HeartBeatTask implements Runnable, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private final int step;

        public HeartBeatTask(int step) {
            this.step = step;
        }

        @Override
        public void run() {
            Thread thread = Thread.currentThread();
            logger.info("I'm a live, step: {}, thread id: {}, name: {}", step, thread.getId(), thread.getName());
        }
    }

    private static void endExecutor(IExecutorService service) {
        service.shutdown();
        try {
            service.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
        service.destroy();
    }
}

