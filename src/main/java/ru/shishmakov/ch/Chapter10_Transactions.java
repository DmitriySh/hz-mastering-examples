package ru.shishmakov.ch;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.hazelcast.core.*;
import com.hazelcast.transaction.HazelcastXAResource;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import java.lang.invoke.MethodHandles;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter10_Transactions {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples() {
        logger.debug("-- Chapter 10. Transactions --");

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

//        simpleTransactionContext(hz1, hz2);
//        useTimeoutTransactionOptions(hz1, hz2);
//        useDurabilityTransactionOptions(hz1, hz2);
//        executeTransactionalTask(hz1, hz2);
//        useXATransaction(hz1, hz2);
        useTimeoutXATransaction(hz1, hz2);
    }

    private static void useTimeoutXATransaction(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ timeoutAction XA Transactions --");

        int timeoutSec = 4;
        IMap<String, Integer> tranMap = hz2.getMap("tranMap");
        IMap<String, Integer> nonTranMap = hz2.getMap("nonTranMap");
        Consumer<BaseMap<String, Integer>> putAction = map -> {
            map.put(UUID.randomUUID().toString(), 100);
            map.put(UUID.randomUUID().toString(), 200);
            map.put(UUID.randomUUID().toString(), 300);
        };
        Consumer<IMap<String, Integer>> checkAction = map -> {
            logger.debug("After action; {}: {}", map.getName(), map.entrySet());
            map.clear();
        };
        Runnable timeoutAction = () -> {
            logger.debug("Timeout start ...  {} sec", timeoutSec);
            try {
                TimeUnit.SECONDS.sleep(timeoutSec * 2);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Error", e);
            }
        };

        fillDataXATransactional(hz1, timeoutSec, putAction, timeoutAction);
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        tranMap.destroy();
        nonTranMap.destroy();
    }

    private static void useXATransaction(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ XA Transactions --");

        int timeoutSec = 30;
        IMap<String, Integer> tranMap = hz2.getMap("tranMap");
        IMap<String, Integer> nonTranMap = hz2.getMap("nonTranMap");
        Consumer<BaseMap<String, Integer>> putAction = map -> {
            map.put(UUID.randomUUID().toString(), 100);
            map.put(UUID.randomUUID().toString(), 200);
            map.put(UUID.randomUUID().toString(), 300);
        };
        Consumer<IMap<String, Integer>> checkAction = map -> {
            logger.debug("After action; {}: {}", map.getName(), map.entrySet());
            map.clear();
        };

        logger.debug("Start success changes ---");
        fillDataXATransactional(hz1, timeoutSec, putAction, () -> logger.debug("Joke Boom! (>_<)"));
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        logger.debug("Start fail changes ---");
        fillDataXATransactional(hz1, timeoutSec, putAction, () -> {
            throw new RuntimeException("Bada BooM!!!");
        });
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);


        tranMap.destroy();
        nonTranMap.destroy();
    }

    private static void executeTransactionalTask(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ execute Transactional Task --");

        IMap<String, Integer> tranMap = hz2.getMap("tranMap");
        IMap<String, Integer> nonTranMap = hz2.getMap("nonTranMap");
        Consumer<BaseMap<String, Integer>> putAction = map -> {
            map.put(UUID.randomUUID().toString(), 100);
            map.put(UUID.randomUUID().toString(), 200);
            map.put(UUID.randomUUID().toString(), 300);
        };
        Consumer<IMap<String, Integer>> checkAction = map -> {
            logger.debug("After action; {}: {}", map.getName(), map.entrySet());
            map.clear();
        };

        logger.debug("Start success changes ---");
        executeTransactionalTask(hz1, putAction, () -> logger.debug("Joke Boom! (>_<)"));
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        logger.debug("Start fail changes ---");
        executeTransactionalTask(hz1, putAction, () -> {
            throw new RuntimeException("Bada BooM!!!");
        });
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        tranMap.destroy();
        nonTranMap.destroy();
    }

    private static void useDurabilityTransactionOptions(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ use Durability Transaction Options --");

        TransactionOptions options = new TransactionOptions()
                .setTimeout(30, TimeUnit.SECONDS)
                .setTransactionType(TransactionType.TWO_PHASE)
                .setDurability(2);
        TransactionContext context = hz1.newTransactionContext(options);

        logger.debug("start transaction");
        context.beginTransaction();
        try {
            TransactionalSet<Integer> tranSet = context.getSet("tranSet");
            TransactionalMap<String, Integer> tranMap = context.getMap("tranMap");
            tranMap.put(UUID.randomUUID().toString(), 100);
            tranMap.put(UUID.randomUUID().toString(), 200);
            tranMap.put(UUID.randomUUID().toString(), 300);
            tranSet.add(100);
            tranSet.add(200);
            tranSet.add(300);

            logger.debug("shutdown hz1 ---");
            for (int i = 0; i < 10; i++) TimeUnit.MILLISECONDS.sleep(100);
            hz1.shutdown();
            for (int i = 0; i < 10; i++) TimeUnit.MILLISECONDS.sleep(300);

            // From logging:
            // INFO  c.h.t.TransactionManagerService - [192.168.1.42]:5702 [dev-hz] [3.6.5]
            // Committing/rolling-back alive transactions of Member [192.168.1.42]:5701, UUID: dd485b70-0d7a-4ac7-bbae-3d6e52fe1942
            //
            // Data should be save on another node, doesn't she?
        } catch (Exception e) {
            logger.error("Error", e);
        }

        IMap<String, Integer> map = hz2.getMap("tranMap");
        ISet<Integer> set = hz2.getSet("tranSet");
        logger.debug("After action; {} size: {}, data set: {}", map.getName(), map.size(), map.entrySet());
        logger.debug("After action; {} size: {}, data set: {}", set.getName(), set.size(), set.toArray());

        map.destroy();
        set.destroy();
    }

    private static void useTimeoutTransactionOptions(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ use Timeout Transaction Options --");

        int configTimeoutSec = 2;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        TransactionOptions options = new TransactionOptions()
                .setTimeout(configTimeoutSec, timeUnit)
                .setTransactionType(TransactionType.TWO_PHASE)
                .setDurability(1);
        TransactionContext context = hz1.newTransactionContext(options);

        logger.debug("Timeout from config: {} sec", configTimeoutSec);
        logger.debug("start transaction");
        context.beginTransaction();
        try {
            TransactionalMap<String, Integer> tranMap = context.getMap("tranMap");
            tranMap.put(UUID.randomUUID().toString(), 100);
            tranMap.put(UUID.randomUUID().toString(), 200);
            tranMap.put(UUID.randomUUID().toString(), 300);

            int actionTimeoutSec = configTimeoutSec * 2;
            logger.debug("Timeout start ...  {} sec", actionTimeoutSec);
            timeUnit.sleep(actionTimeoutSec);

            logger.debug("commit transaction");
            context.commitTransaction();
        } catch (Exception e) {
            logger.error("rollback transaction", e);
            context.rollbackTransaction();
        }

        IMap<String, Integer> map = hz2.getMap("tranMap");
        logger.debug("After action; {}: {}", map.getName(), map.entrySet());

        map.destroy();
    }

    private static void simpleTransactionContext(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ simple Transaction Context --");

        IMap<String, Integer> tranMap = hz2.getMap("tranMap");
        IMap<String, Integer> nonTranMap = hz2.getMap("nonTranMap");
        Consumer<BaseMap<String, Integer>> putAction = map -> {
            map.put(UUID.randomUUID().toString(), 100);
            map.put(UUID.randomUUID().toString(), 200);
            map.put(UUID.randomUUID().toString(), 300);
        };
        Consumer<IMap<String, Integer>> checkAction = map -> {
            logger.debug("After action; {}: {}", map.getName(), map.entrySet());
            map.clear();
        };


        logger.debug("Start success changes ---");
        fillDataTransactional(hz1, putAction, () -> logger.debug("Joke Boom! (>_<)"));
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        logger.debug("Start fail changes ---");
        fillDataTransactional(hz1, putAction, () -> {
            throw new RuntimeException("Bada BooM!!!");
        });
        checkAction.accept(tranMap);
        checkAction.accept(nonTranMap);

        tranMap.destroy();
        nonTranMap.destroy();
    }

    private static void fillDataXATransactional(HazelcastInstance hz, int timeoutSec,
                                                Consumer<BaseMap<String, Integer>> putAction, Runnable mine) {
        HazelcastXAResource xaResource = hz.getXAResource();
        UserTransactionManager tm = new UserTransactionManager();
        try {
            logger.debug("start transaction");
            tm.setTransactionTimeout(timeoutSec);
            tm.begin();
            Transaction transaction = tm.getTransaction();
            transaction.enlistResource(xaResource);
            TransactionContext context = xaResource.getTransactionContext();

            TransactionalMap<String, Integer> tranMap = context.getMap("tranMap");
            IMap<String, Integer> nonTranMap = hz.getMap("nonTranMap");
            putAction.accept(tranMap);
            putAction.accept(nonTranMap);
            mine.run();

            transaction.delistResource(xaResource, XAResource.TMSUCCESS);
            logger.debug("commit transaction");
            tm.commit();
        } catch (Exception e) {
            try {
                logger.debug("rollback transaction", e);
                tm.rollback();
            } catch (Exception ex) {
                logger.debug("Error", ex);
            }
        }
    }

    private static void fillDataTransactional(HazelcastInstance hz,
                                              Consumer<BaseMap<String, Integer>> action, Runnable mine) {
        TransactionContext context = hz.newTransactionContext();
        logger.debug("start transaction");
        context.beginTransaction();
        try {
            TransactionalMap<String, Integer> tranMap = context.getMap("tranMap");
            IMap<String, Integer> nonTranMap = hz.getMap("nonTranMap");

            action.accept(tranMap);
            action.accept(nonTranMap);
            mine.run();

            logger.debug("commit transaction");
            context.commitTransaction();
        } catch (Exception e) {
            logger.error("rollback transaction", e);
            context.rollbackTransaction();
        }
    }

    private static void executeTransactionalTask(HazelcastInstance hz1, Consumer<BaseMap<String, Integer>> putAction,
                                                 Runnable mine) {
        try {
            hz1.executeTransaction(context -> {
                logger.debug("start into transaction");

                TransactionalMap<String, Integer> tranMap = context.getMap("tranMap");
                IMap<String, Integer> nonTranMap = hz1.getMap("nonTranMap");
                putAction.accept(tranMap);
                putAction.accept(nonTranMap);
                mine.run();

                logger.debug("end into transaction");
                return null;
            });
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

}
