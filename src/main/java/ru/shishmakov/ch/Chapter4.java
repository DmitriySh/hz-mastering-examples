package ru.shishmakov.ch;

import com.hazelcast.core.*;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.ReadResultSet;
import com.hazelcast.ringbuffer.Ringbuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter4 {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int POISON_PILL = -1;

    public static void doExampels(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {

        iQueue(hz1, service);
        iListAndISet(hz1, hz2);
        ringBuffer1(hz1);
        ringBuffer2(hz1);

    }

    private static void iQueue(HazelcastInstance hz1, ExecutorService service) {
        logger.debug(" -- IQueue -- ");
        try {
            IQueue<Integer> queue = hz1.getQueue("queue");
            queue.addItemListener(new QueueItemListener(), true);
            List<Callable<Integer>> tasks = new ArrayList<>();
            tasks.add(new Producer(hz1));
            for (int i = 0; i < 5; i++) tasks.add(new Consumer(hz1));
            service.invokeAll(tasks);
        } catch (InterruptedException e) {
            logger.error("Error", e);
        }
    }

    private static void iListAndISet(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug(" -- IList -- ");
        IList<String> list = hz1.getList("list");
        for (String item : Arrays.asList("I", "am", "distributed", "IList")) list.add(item);
        for (Object item : hz2.getList("list")) logger.debug("List item: {}", item);

        logger.debug(" -- ISet -- ");
        ISet<String> set = hz1.getSet("set");
        for (String item : Arrays.asList("I", "am", "distributed", "ISet")) set.add(item);
        for (Object item : hz2.getSet("set")) logger.debug("Set item: {}", item);
    }

    private static void ringBuffer1(HazelcastInstance hz1) {
        logger.debug(" -- Ringbuffer 1 -- ");
        Ringbuffer<Integer> rb1 = hz1.getRingbuffer("ring1");
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        try {
            logger.debug("Fill ringBuffer: {}, in: {}", rb1.getName(), list);
            for (Integer item : list) {
                if (rb1.addAsync(item, OverflowPolicy.FAIL).get() == -1) {
                    logger.warn("RingBuffer: {} can not add all items, ringCapacity: {}, listSize: {}",
                            rb1.getName(), rb1.capacity(), list.size());
                    break;
                }
            }

            logger.debug("RingBuffer: {} size: {} capacity: {}", rb1.getName(), rb1.size(), rb1.capacity());
            ReadResultSet<Integer> resultSet = rb1.readManyAsync(
                    rb1.headSequence(),
                    (int) rb1.tailSequence(),
                    (int) rb1.capacity(), null).get();
            List<Integer> result = new ArrayList<>(resultSet.readCount());
            resultSet.forEach(result::add);
            logger.debug("Read ringBuffer: {}, out: {}", rb1.getName(), result);
        } catch (Exception e) {
            logger.debug("Error", e);
        }
    }

    private static void ringBuffer2(HazelcastInstance hz1) {
        logger.debug(" -- Ringbuffer 2 -- ");
        Ringbuffer<Integer> rb2 = hz1.getRingbuffer("ring2");
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        try {
            logger.debug("Fill ringBuffer: {}, in: {}", rb2.getName(), list);
            if (rb2.addAllAsync(list, OverflowPolicy.FAIL).get() == -1) {
                logger.warn("RingBuffer: {} can not add all items, ringCapacity: {}, listSize: {}",
                        rb2.getName(), rb2.capacity(), list.size());
                return;
            }

            logger.debug("RingBuffer: {} size: {} capacity: {}", rb2.getName(), rb2.size(), rb2.capacity());
            ReadResultSet<Integer> resultSet = rb2.readManyAsync(
                    rb2.headSequence(),
                    (int) rb2.tailSequence(),
                    (int) Math.min(1000, rb2.capacity()), null).get();
            List<Integer> result = new ArrayList<>(resultSet.readCount());
            resultSet.forEach(result::add);
            logger.debug("Read ringBuffer: {}, out: {}", rb2.getName(), result);
        } catch (Exception e) {
            logger.debug("Error", e);
        }
    }

    private static class Producer implements Callable<Integer> {

        private final HazelcastInstance hz1;

        Producer(HazelcastInstance hz1) {
            this.hz1 = hz1;
        }

        @Override
        public Integer call() throws Exception {
            IQueue<Integer> queue = hz1.getQueue("queue");
            for (int i = 0; i < 25; i++) {
                queue.put(i);
                Thread.sleep(100);
            }
            queue.put(POISON_PILL);
            return null;
        }
    }

    private static class Consumer implements Callable<Integer> {

        private static int consumers = 1;
        private final int id = consumers++;
        private final HazelcastInstance hz1;

        Consumer(HazelcastInstance hz1) {
            this.hz1 = hz1;
        }

        @Override
        public Integer call() throws Exception {
            IQueue<Integer> queue = hz1.getQueue("queue");
            for (; ; ) {
                int item = queue.take();
                Thread.sleep(200);
//                logger.debug("Consumer {} take: {}", id, item);
                if (item == POISON_PILL) {
//                    logger.debug("Consumer {} end!", id);
                    queue.put(POISON_PILL);
                    break;
                }
            }
            return null;
        }
    }

    private static class QueueItemListener implements com.hazelcast.core.ItemListener<Integer> {
        @Override
        public void itemAdded(ItemEvent<Integer> item) {
            if (item.getItem() == POISON_PILL) logger.debug("Producer end; put POISON_PILL {}", POISON_PILL);
            else logger.debug("Producer put: {}", item.getItem());
        }

        @Override
        public void itemRemoved(ItemEvent<Integer> item) {
            if (item.getItem() == POISON_PILL) logger.debug("Consumer end!");
            else logger.debug("Consumer take: {}", item.getItem());
        }
    }
}
