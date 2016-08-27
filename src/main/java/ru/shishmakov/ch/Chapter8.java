package ru.shishmakov.ch;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static ru.shishmakov.Main.buildHZClientInstance;

/**
 * Created by dima on 21.08.16.
 */
public class Chapter8 {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Integer POISON_PILL = -1;

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        HazelcastInstance hzClient = buildHZClientInstance();

        exchangeClientAndCluster(hz1, hz2, hzClient);


        HazelcastClient.shutdownAll();
    }

    private static void exchangeClientAndCluster(HazelcastInstance hz1, HazelcastInstance hz2, HazelcastInstance hzClient) {
        logger.debug("-- HZ client use cluster --");

        IQueue<Integer> queueNode2 = hz2.getQueue("queue");
        try {
            for (int i = 0; i < 10; i++) {
                queueNode2.put(i);
                logger.debug("queueNode2 put --> {}", i);
            }
            queueNode2.put(POISON_PILL);
            logger.debug("queueNode2 put --> POISON_PILL={}; last item", POISON_PILL);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }

        IQueue<Integer> queueClient = hzClient.getQueue("queue");
        try {
            for (int threshold = 100; threshold > 0; threshold--) {
                int item = queueClient.poll(500, TimeUnit.MILLISECONDS);
                if (POISON_PILL.equals(item)) {
                    logger.debug("queueClient poll x-- POISON_PILL={}; exit", item);
                    break;
                }
                logger.debug("queueClient poll <-- {}", item);
            }
            logger.debug("queueClient end iteration");
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }
}
