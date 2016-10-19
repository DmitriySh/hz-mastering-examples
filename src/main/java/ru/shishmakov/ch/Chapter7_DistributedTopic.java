package ru.shishmakov.ch;

import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;
import com.hazelcast.topic.ReliableMessageListener;
import com.hazelcast.util.executor.StripedExecutor;
import com.hazelcast.util.executor.StripedRunnable;
import com.hazelcast.util.executor.TimeoutRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter7_DistributedTopic {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String RELIABLE_TOPIC = "reliableTopic";
    private static final String TOPIC_DEFAULT = "topicDefault";
    private static final String RECEIVER = "receiver";
    private static final String SUBSCRIBER = "subscriber";

    public static void doExamples(ExecutorService service) {
        logger.debug("-- Chapter 7. Distributed Topic --");

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

//        topicPublishMultipleSubscribes(hz1, hz2);
//        reliableTopicPublishSubscribe(hz1, hz2);
        topicStripedExecutor(hz1, hz2);
    }

    private static void topicStripedExecutor(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- ITopic striped executor --");

        final String message = "Hello!";
        ICountDownLatch receiver = hz1.getCountDownLatch(RECEIVER);
        ICountDownLatch subscriber = hz1.getCountDownLatch(SUBSCRIBER);
        receiver.trySetCount(1);
        subscriber.trySetCount(1);

        IExecutorService service = hz2.getExecutorService("executor");
        service.execute(new StripedSubscriber());
        service.execute(new StripedPublisher(message));

        try {
            receiver.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }

        ITopic<Object> topic = hz2.getTopic(TOPIC_DEFAULT);
        long publishCnt = topic.getLocalTopicStats().getPublishOperationCount();
        long receiveCnt = topic.getLocalTopicStats().getReceiveOperationCount();
        logger.debug("publishCnt: {}, receiveCnt: {}", publishCnt, receiveCnt);

        topic.destroy();
        receiver.destroy();
        endExecutor(service);
    }

    private static void reliableTopicPublishSubscribe(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- ITopic reliable publish subscribe --");

        String[] messages = {"one", "two", "three"};
        ICountDownLatch receiver = hz1.getCountDownLatch(RECEIVER);
        receiver.trySetCount(messages.length);

        ITopic<Object> topic = hz2.getReliableTopic(RELIABLE_TOPIC);
        topic.addMessageListener(new MyReliableMessageListener(receiver));
        Stream.of(messages).forEach(m -> {
            topic.publish(m);
            logger.debug("--> push msg: {}", m);
        });

        try {
            receiver.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
        long publishCnt = topic.getLocalTopicStats().getPublishOperationCount();
        long receiveCnt = topic.getLocalTopicStats().getReceiveOperationCount();
        logger.debug("publishCnt: {}, receiveCnt: {}", publishCnt, receiveCnt);

        topic.destroy();
        receiver.destroy();
    }

    private static void topicPublishMultipleSubscribes(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- ITopic publish subscribe --");

        final int subCnt = 2;
        ICountDownLatch receiver = hz1.getCountDownLatch(RECEIVER);
        ICountDownLatch subscriber = hz1.getCountDownLatch(SUBSCRIBER);
        receiver.trySetCount(subCnt * 5);
        subscriber.trySetCount(subCnt);

        ITopic<Object> topic = hz2.getTopic(TOPIC_DEFAULT);
        IExecutorService service = hz2.getExecutorService("executor");
        IntStream.range(0, subCnt).forEach(t -> service.execute(new Subscriber()));
        service.execute(new Publisher(subCnt));

        try {
            receiver.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }

        long publishCnt = topic.getLocalTopicStats().getPublishOperationCount();
        long receiveCnt = topic.getLocalTopicStats().getReceiveOperationCount();
        logger.debug("publishCnt: {}, receiveCnt: {}", publishCnt, receiveCnt);

        receiver.destroy();
        subscriber.destroy();
        endExecutor(service);
    }


    public static class Subscriber implements Runnable, Serializable, HazelcastInstanceAware {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private static AtomicInteger counter = new AtomicInteger(0);
        private int number = counter.getAndIncrement();

        private transient HazelcastInstance hz;

        @Override
        public void run() {
            ITopic<Object> topic = hz.getTopic(TOPIC_DEFAULT);
            topic.addMessageListener(new SubListener(hz, number));
            notifySubscriberGetReady();
        }

        private void notifySubscriberGetReady() {
            ICountDownLatch subscriber = hz.getCountDownLatch(SUBSCRIBER);
            subscriber.countDown();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        public static class SubListener implements MessageListener<Object> {
            private final ICountDownLatch receiver;
            private final int number;

            public SubListener(HazelcastInstance hz, int number) {
                this.receiver = hz.getCountDownLatch(RECEIVER);
                this.number = number;
            }

            @Override
            public void onMessage(Message<Object> m) {
                logger.debug("Subscriber {} receive <-- {}; source: {}, publisher: {}", number, m.getMessageObject(), m.getSource(),
                        m.getPublishingMember());
                receiver.countDown();
            }
        }
    }

    public static class Publisher implements Runnable, Serializable, HazelcastInstanceAware {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private transient HazelcastInstance hz;
        private final int subCnt;

        public Publisher(int subCnt) {
            this.subCnt = subCnt;
        }

        @Override
        public void run() {
            waitGetReadySubscribers();
            ITopic<Object> topic = hz.getTopic(TOPIC_DEFAULT);
            ICountDownLatch receiver = hz.getCountDownLatch(RECEIVER);
            IntStream.range(0, receiver.getCount() / subCnt).forEach(t -> {
                try {
                    topic.publish(t);
                    logger.debug("Publish --> {}", t);
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    logger.debug("Error", e);
                }
            });

        }

        private void waitGetReadySubscribers() {
            ICountDownLatch subscriber = hz.getCountDownLatch(SUBSCRIBER);
            try {
                subscriber.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.debug("Error", e);
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }
    }

    public static class StripedPublisher implements Runnable, HazelcastInstanceAware, Serializable {
        private final String message;
        private HazelcastInstance hz;

        public StripedPublisher(String message) {
            this.message = message;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            waitGetReadySubscribers();
            ITopic<Object> topic = hz.getTopic(TOPIC_DEFAULT);
            topic.publish(message);
            logger.debug("--> push msg: {}", message);
        }

        private void waitGetReadySubscribers() {
            ICountDownLatch subscriber = hz.getCountDownLatch(SUBSCRIBER);
            try {
                subscriber.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.debug("Error", e);
            }
        }
    }

    public static class StripedSubscriber implements Runnable, HazelcastInstanceAware, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private transient HazelcastInstance hz;

        @Override
        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            ITopic<Object> topic = hz.getTopic(TOPIC_DEFAULT);
            topic.addMessageListener(new StripedListener(hz));
            notifySubscriberGetReady();
        }

        private void notifySubscriberGetReady() {
            ICountDownLatch subscriber = hz.getCountDownLatch(SUBSCRIBER);
            subscriber.countDown();
        }

        public static class StripedListener implements MessageListener<Object> {
            private static final ILogger ilogger = com.hazelcast.logging.Logger.getLogger(MethodHandles.lookup().lookupClass());
            private static final StripedExecutor executor = new StripedExecutor(ilogger, "striped logger", null, 10, 1_000);

            private final HazelcastInstance hz;

            public StripedListener(HazelcastInstance hz) {
                this.hz = hz;
            }

            @Override
            public void onMessage(Message<Object> m) {
                executor.execute(new StripedTask(m));
                hz.getCountDownLatch(RECEIVER).countDown();
                executor.shutdown();
            }
        }

        public static class StripedTask implements StripedRunnable, TimeoutRunnable, Serializable {
            private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

            private final Message<Object> msg;

            public StripedTask(Message<Object> msg) {
                this.msg = msg;
            }

            @Override
            public void run() {
                logger.debug("<-- receive msg: {}, source: {}", msg.getMessageObject(), msg.getSource());
            }

            @Override
            public int getKey() {
                return msg.getSource().hashCode();
            }

            @Override
            public long getTimeout() {
                return 1;
            }

            @Override
            public TimeUnit getTimeUnit() {
                return TimeUnit.SECONDS;
            }
        }
    }

    public static class MyReliableMessageListener implements ReliableMessageListener<Object> {
        private final ICountDownLatch receiver;

        public MyReliableMessageListener(ICountDownLatch receiver) {
            this.receiver = receiver;
        }

        @Override
        public long retrieveInitialSequence() {
            return -1;
        }

        @Override
        public void storeSequence(long sequence) {

        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }

        @Override
        public void onMessage(Message<Object> m) {
            logger.debug("<-- receive msg: {}, source: {}", m.getMessageObject(), m.getSource());
            receiver.countDown();
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
