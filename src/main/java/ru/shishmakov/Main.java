package ru.shishmakov;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter11;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String GROUP_NAME = "dev-hz";
    public static final String GROUP_PASSWORD = "dev-hz";

    private static ExecutorService service = Executors.newCachedThreadPool();


    public static void main(String[] args) throws InterruptedException {

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

        try {
//            Chapter2.doExamples(hz1, hz2);
//            Chapter3.doExamples(hz1, hz2, service);
//            Chapter4.doExamples(hz1, hz2, service);
//            Chapter5.doExamples(hz1, hz2, service);
//            Chapter6.doExamples(hz1, hz2, service);
//            Chapter7.doExamples(hz1, hz2, service);
//            Chapter8.doExamples(hz1, hz2, service);
//            Chapter9.doExamples(hz1, hz2);
//            Chapter10.doExamples(hz1, hz2);
            Chapter11.doExamples(hz1, hz2);
        } finally {
            service.shutdownNow();
            service.awaitTermination(15, TimeUnit.SECONDS);
            Hazelcast.shutdownAll();
        }
    }
}
