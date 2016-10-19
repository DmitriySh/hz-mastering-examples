package ru.shishmakov;

import com.hazelcast.core.Hazelcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter16_ExtendingHazelcast;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String GROUP_NAME = "dev-hz";
    public static final String GROUP_PASSWORD = "dev-hz";

    private static ExecutorService service = Executors.newCachedThreadPool();


    public static void main(String[] args) throws InterruptedException {
        try {
//            Chapter2_LearningTheBasics.doExamples();
//            Chapter3_DistributedPrimitives.doExamples(service);
//            Chapter4_DistributedCollections.doExamples(service);
//            Chapter5_DistributedMap.doExamples(service);
//            Chapter6_DistributedExecutorService.doExamples(service);
//            Chapter7_DistributedTopic.doExamples(service);
//            Chapter8_HazelcastClients.doExamples();
//            Chapter9_Serialization.doExamples();
//            Chapter10_Transactions.doExamples();
//            Chapter11_JCacheProvider.doExamples();
            Chapter16_ExtendingHazelcast.doExamples();
        } finally {
            service.shutdownNow();
            service.awaitTermination(5, TimeUnit.SECONDS);
            Hazelcast.shutdownAll();
        }
    }
}
