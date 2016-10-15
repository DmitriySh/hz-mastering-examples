package ru.shishmakov.ch;

import com.hazelcast.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter3_DistributedPrimitives {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExampels(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- Chapter 3. Distributed Primitives --");

        atomicLong(hz1);
        generator(hz1, hz2);
        atomicReference(hz2);
        lock(hz1, service);
        semaphore(hz2, service);
        countDownLatch(hz1, hz2, service);
    }

    private static void atomicLong(HazelcastInstance hz1) {
        logger.debug(" -- IAtomicLong -- ");
        IAtomicLong atomicNode1 = hz1.getAtomicLong("atomicLong");
        IAtomicLong atomicNode2 = hz1.getAtomicLong("atomicLong");
        logger.debug("Atomic value before set; value: {}", atomicNode1.get());
        atomicNode1.set(1);
        logger.debug("Atomic node1 value after set; value: {}", atomicNode1.get());
        logger.debug("Atomic node1 value getAndAdd; value: {}", atomicNode1.getAndAdd(2));
        logger.debug("Atomic node1 value get; value: {}", atomicNode1.get());
        logger.debug("Atomic node1 value alterAndGet; value: {}", atomicNode1.alterAndGet(t -> t + 2));
        logger.debug("Atomic node2 value get; value: {}", atomicNode2.get());
        atomicNode2.alterAndGet(t -> t + 2);
        logger.debug("Atomic node2 value alterAndGet; node2 value: {}, node1 value: {}", atomicNode2.get(), atomicNode1.get());
    }

    private static void generator(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug(" -- IdGenerator -- ");
        IdGenerator primaryGenNode1 = hz1.getIdGenerator("primaryGen");
        IdGenerator primaryGenNode2 = hz2.getIdGenerator("primaryGen");
        for (int i = 0; i < 50; i++) {
            long id1 = primaryGenNode1.newId();
            long id2 = primaryGenNode2.newId();
            logger.debug("The same generator; node1: {}, node2: {}", id1, id2);
        }
    }

    private static void atomicReference(HazelcastInstance hz2) {
        logger.debug(" -- IAtomicReference -- ");
        IAtomicReference<MyStore> atomicRefNode2 = hz2.getAtomicReference("atomicRef");
        atomicRefNode2.set(new MyStore(1));
        logger.debug("AtimicRef node2; get value: {}", atomicRefNode2.get());
        logger.debug("AtimicRef node2; alter and get value: {}", atomicRefNode2.alterAndGet(m -> {
            m.incrementCounter();
            return m;
        }));
    }

    private static void lock(HazelcastInstance hz1, ExecutorService service) {
        logger.debug(" -- ILock -- ");
        IAtomicLong atomicF1 = hz1.getAtomicLong("atomicLong@foo");
        IAtomicLong atomicF2 = hz1.getAtomicLong("atomicLong@bar");
        atomicF1.set(1);
        atomicF2.set(1);
        final int count = 20;
        ILock lock = hz1.getLock("myLock");
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int number = i;
            service.submit(() -> {
                for (; ; ) if (lock.tryLock(350, TimeUnit.MILLISECONDS)) break;
                try {
                    logger.debug("lock: {} has been acquired", number);
                    atomicF1.alter(t -> t + 5);
                    Thread.sleep(250);
                    atomicF2.alter(t -> t + 5);
                    Thread.sleep(50);
                    logger.debug("atomicF1 value: {}, atomicF2 value: {}", atomicF1.get(), atomicF2.get());
                } finally {
                    logger.debug("lock: {} has been released", number);
                    lock.unlock();
                }
                latch.countDown();
                return null;
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Error", e);
        }
    }

    private static void semaphore(HazelcastInstance hz2, ExecutorService service) {
        logger.debug(" -- ISemaphore -- ");
        ISemaphore semaphore = hz2.getSemaphore("semph");
        IAtomicLong resource = hz2.getAtomicLong("resource");
        logger.debug("semaphore capacity: {}", semaphore.availablePermits());
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        for (int k = 0; k < count; k++) {
            int number = k;
            service.submit(() -> {
                logger.debug("iter: {}, active threads: {}", number, resource.get());
                semaphore.acquire();
                try {
                    resource.incrementAndGet();
                    Thread.sleep(250);
                    resource.decrementAndGet();
                } finally {
                    semaphore.release();
                }
                latch.countDown();
                return null;
            });
        }
        try {
            latch.await();
            semaphore.destroy();
        } catch (InterruptedException e) {
            logger.error("Error", e);
        }
    }

    private static void countDownLatch(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug(" -- countDownLatch -- ");
        final int count = 7;
        ICountDownLatch masterLatch = hz1.getCountDownLatch("masterLatch");
        ICountDownLatch slaveLatch = hz1.getCountDownLatch("slaveLatch");
        if (slaveLatch.trySetCount(count) && masterLatch.trySetCount(1)) {
            logger.debug("slaveLatch get ready: {}", slaveLatch.getCount());
            logger.debug("masterLatch get ready: {}", masterLatch.getCount());
            try {
                List<Callable<Void>> tasks = new ArrayList<>();
                tasks.add(new Master(hz1));
                for (int i = 0; i < count; i++) tasks.add(new Slave(hz1));
                service.invokeAll(tasks);
            } catch (InterruptedException e) {
                logger.error("Error", e);
            }
        }
        masterLatch.destroy();
        slaveLatch.destroy();
    }

    public static class Master implements Callable<Void> {

        private final ICountDownLatch masterLatch;
        private final ICountDownLatch slaveLatch;
        private final IAtomicLong counter;

        public Master(HazelcastInstance hz1) {
            this.masterLatch = hz1.getCountDownLatch("masterLatch");
            this.slaveLatch = hz1.getCountDownLatch("slaveLatch");
            this.counter = hz1.getAtomicLong("latchCounter");
        }

        public Void call() {
            try {
                logger.debug("Master wait slaves (slaveLatch: {}) ...", slaveLatch.getCount());
                slaveLatch.await(1, TimeUnit.MINUTES);
                logger.debug("Master do action!)");
                masterLatch.countDown();
                logger.debug("Master end ...");
            } catch (InterruptedException e) {
                logger.error("Error", e);
            }
            return null;
        }
    }

    public static class Slave implements Callable<Void> {

        private final long id;
        private final ICountDownLatch masterLatch;
        private final ICountDownLatch slaveLatch;
        private final IAtomicLong counter;

        public Slave(HazelcastInstance hz1) {
            this.masterLatch = hz1.getCountDownLatch("masterLatch");
            this.slaveLatch = hz1.getCountDownLatch("slaveLatch");
            this.counter = hz1.getAtomicLong("latchCounter");
            this.id = hz1.getIdGenerator("latchGenerator").newId();
        }

        public Void call() {
            try {
                logger.debug("Slave: {} has created", id);
                counter.alter(t -> t + 1);
                slaveLatch.countDown();
                logger.debug("Slave: {} wait master!", id);
                masterLatch.await(1, TimeUnit.MINUTES);
                logger.debug("Slave: {} end ...", id);
            } catch (InterruptedException e) {
                logger.error("Error", e);
            }
            return null;
        }
    }


    public static class MyStore implements Serializable {
        int counter;

        public MyStore(int counter) {
            this.counter = counter;
        }

        public int getCounter() {
            return counter;
        }

        public int incrementCounter() {
            return ++counter;
        }

        @Override
        public String toString() {
            return "MyStore{" +
                    "counter=" + counter +
                    '}';
        }
    }
}
