package ru.shishmakov.ch;

import com.hazelcast.core.*;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.mapreduce.*;
import com.hazelcast.mapreduce.aggregation.Aggregations;
import com.hazelcast.mapreduce.aggregation.PropertyExtractor;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.query.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.time.Month;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.time.Month.*;
import static ru.shishmakov.hz.cfg.HzClusterConfig.buildHZInstance;

/**
 * @author Dmitriy Shishmakov
 */
public class Chapter5_DistributedMap {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String DIMA = "Dima";
    public static final String IGORE = "Igore";
    public static final String SASHA = "Sasha";
    public static final String GALYA = "Galya";
    public static final String SHISHMAKOV = "Shishmakov";

    public static void doExamples(ExecutorService service) {
        logger.debug("-- Chapter 5. Distributed Map --");

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

//        mapGetCopy(hz1, hz2);
//        mapMemoryFormat(hz1, hz2);
//        mapPartitionAware(hz1, hz2);
//        mapMaxSizeEviction(hz1, hz2);
//        mapIdleTimeoutEviction(hz1, hz2);
//        mapBackup(hz1, hz2);
//        mapNearCash(hz1, hz2);
//        mapLock(hz1, hz2, service);
//        mapLeaseLock(hz1, hz2, service);
//        mapReentrantLock(hz1, hz2, service);
//        mapDestroyLock(hz1, hz2, service);
//        mapEntryProcessor(hz1, hz2, service);
//        mapListeners(hz1, hz2, service);
//        mapDistributedQueries(hz1, hz2, service);
//        mapMapReduceNumberSum(hz1, hz2, service);
//        mapMapReduceWordCounter(hz1, hz2, service);
//        mapAggregations(hz1, hz2, service);
        mapMapStore(hz1, hz2, service);
//        mapMultiMap(hz1, hz2, service);
//        mapReplicatedMap(hz1, hz2, service);
    }

    private static void mapReplicatedMap(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap ReplicatedMap --");

        ReplicatedMap<Integer, Integer> repMap1Node1 = hz2.getReplicatedMap("replicatedMap1");
        ReplicatedMap<Integer, Integer> repMap1Node2 = hz2.getReplicatedMap("replicatedMap1");

        IntStream.range(0, 10)
                .forEach(t -> {
                    logger.debug("put key: {}, value: {}", t, t);
                    repMap1Node1.put(t, t);
                });
        logger.debug("After puts; size: {}, entrySet: {}", repMap1Node1.size(), repMap1Node1.entrySet());

        repMap1Node1.destroy();
        repMap1Node2.destroy();
    }

    private static void mapMultiMap(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap MultiMap --");

        String key = "ids";
        MultiMap<String, Integer> multiMap1 = hz1.getMultiMap("multiMap1");
        Collection<Integer> ids = multiMap1.get(key);
        logger.debug("contains key: {}, values: {}", multiMap1.containsKey(key), ids);

        IntStream.range(0, 10)
                .flatMap(t -> IntStream.of(t, t))
                .forEach(t -> {
                    logger.debug("put key: {}, value: {}", key, t);
                    multiMap1.put(key, t);
                });
        logger.debug("After puts; key: {}, value: {}", key, multiMap1.get(key));

        multiMap1.remove(key, 9);
        logger.debug("remove value: {}, size: {}", 9, multiMap1.size());

        multiMap1.put(key, 10);
        logger.debug("After put; key: {}, value: {}", key, multiMap1.get(key));

        multiMap1.destroy();
    }

    private static void mapMapStore(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap MapStore --");

        IMap<String, Employee2> employees = hz1.getMap("mapMapStore");
        logger.debug("size: {}", employees.size());

        logger.debug("put new data");
        employees.set("Kesha", new Employee2("Kesha", SHISHMAKOV));
        logger.debug("after put size: {}", employees.size());

        employees.get(DIMA);
        Employee2 mashaEmployee = employees.get("Masha");
        logger.debug("after get size: {}", employees.size());

        employees.delete(DIMA);
        logger.debug("after delete size: {}", employees.size());

        Map<String, Employee2> map = new HashMap<>();
        employees.entrySet().forEach(e -> map.put(e.getKey(), e.getValue()));

        employees.clear();
        logger.debug("after clear size: {}", employees.size());

        employees.putAll(map);
        logger.debug("after putAll size: {}", employees.size());

        employees.destroy();
    }

    private static void mapAggregations(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap Aggregations --");

        IMap<String, Employee2> employees = hz1.getMap("employees");
        IMap<String, SalaryYear> salaries = hz2.getMap("salaries");

        employees.set(DIMA, new Employee2(DIMA, SHISHMAKOV));
        employees.set(IGORE, new Employee2(IGORE, SHISHMAKOV));
        employees.set(SASHA, new Employee2(SASHA, SHISHMAKOV));
        employees.set(GALYA, new Employee2(GALYA, SHISHMAKOV));

        List<SalaryMonth> dMonths = Arrays.asList(new SalaryMonth(JANUARY, 100),
                new SalaryMonth(FEBRUARY, 100), new SalaryMonth(MARCH, 100), new SalaryMonth(APRIL, 100),
                new SalaryMonth(MAY, 100), new SalaryMonth(JUNE, 100), new SalaryMonth(JULY, 100),
                new SalaryMonth(AUGUST, 100), new SalaryMonth(SEPTEMBER, 100), new SalaryMonth(OCTOBER, 100),
                new SalaryMonth(NOVEMBER, 100), new SalaryMonth(DECEMBER, 100));
        List<SalaryMonth> iMonths = Arrays.asList(new SalaryMonth(JANUARY, 100),
                new SalaryMonth(FEBRUARY, 200), new SalaryMonth(MARCH, 300), new SalaryMonth(APRIL, 400),
                new SalaryMonth(MAY, 500), new SalaryMonth(JUNE, 600), new SalaryMonth(JULY, 700),
                new SalaryMonth(AUGUST, 800), new SalaryMonth(SEPTEMBER, 900), new SalaryMonth(OCTOBER, 1000),
                new SalaryMonth(NOVEMBER, 1100), new SalaryMonth(DECEMBER, 1200));
        List<SalaryMonth> sMonths = Arrays.asList(new SalaryMonth(JANUARY, 100),
                new SalaryMonth(FEBRUARY, 0), new SalaryMonth(MARCH, 200), new SalaryMonth(APRIL, 0),
                new SalaryMonth(MAY, 300), new SalaryMonth(JUNE, 0), new SalaryMonth(JULY, 400),
                new SalaryMonth(AUGUST, 0), new SalaryMonth(SEPTEMBER, 500), new SalaryMonth(OCTOBER, 0),
                new SalaryMonth(NOVEMBER, 600), new SalaryMonth(DECEMBER, 0));
        List<SalaryMonth> gMonths = Arrays.asList(new SalaryMonth(JANUARY, 100),
                new SalaryMonth(FEBRUARY, 100), new SalaryMonth(MARCH, 100), new SalaryMonth(APRIL, 100),
                new SalaryMonth(MAY, 100), new SalaryMonth(JUNE, 100), new SalaryMonth(JULY, 100),
                new SalaryMonth(AUGUST, 100), new SalaryMonth(SEPTEMBER, 100), new SalaryMonth(OCTOBER, 100),
                new SalaryMonth(NOVEMBER, 100), new SalaryMonth(DECEMBER, 100));
        salaries.set(DIMA, new SalaryYear("dima@mail.ru", 2010, dMonths));
        salaries.set(IGORE, new SalaryYear("igore@mail.ru", 2010, iMonths));
        salaries.set(SASHA, new SalaryYear("sasha@mail.ru", 2010, sMonths));
        salaries.set(GALYA, new SalaryYear("galya@mail.ru", 2010, gMonths));

        PropertyExtractor<SalaryYear, Integer> extractor = SalaryYear::getAnnualSalary;
        Integer avg = salaries.aggregate(Supplier.all(extractor), Aggregations.integerAvg());
        logger.debug("avg annual salary: {}", avg);

        Long count = employees.aggregate(Supplier.all(), Aggregations.count());
        logger.debug("employees count: {}", count);

        employees.destroy();
        salaries.destroy();
    }

    private static void mapMapReduceWordCounter(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap MapReduce word counter --");

        final String[] text = {"Saturn is a planet", "Earth is a planet", "Pluto is not a planet anymore"};

        IMap<Integer, String> map = hz1.getMap("mrMap");
        for (int n = 0; n < text.length; n++) {
            map.set(n, text[n]);
        }

        JobTracker tracker = hz1.getJobTracker("jobTracker1");
        Job<Integer, String> job = tracker.newJob(KeyValueSource.fromMap(map));
        JobCompletableFuture<List<Map.Entry<String, Long>>> future = job
                .onKeys(map.keySet())
                .mapper(new WordMapper())
                .combiner(new WordCombiner())
                .reducer(new WordReducer())
                .submit(new WordCollator());

        try {
            List<Map.Entry<String, Long>> result = future.get();
            result.forEach(v -> {
                logger.debug("word: {}", v);
            });
        } catch (InterruptedException | ExecutionException e) {
            logger.debug("Error", e);
        }
    }

    private static void mapMapReduceNumberSum(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap MapReduce number sum --");

        IMap<Integer, Long> map = hz1.getMap("mrMap");
        int[] manualCnt = new int[]{0};
        IntStream.range(0, 1_000).forEach(i -> {
            map.set(i, (long) i);
            manualCnt[0] += i;
        });
        logger.debug("before manualCnt = {}", manualCnt[0]);

        logger.debug("Start MapReduce task ...");
        JobTracker tracker = hz2.getJobTracker("jobTracker1");
        Job<Integer, Long> job1 = tracker.newJob(KeyValueSource.fromMap(map));
        Job<Integer, Long> job2 = tracker.newJob(KeyValueSource.fromMap(map));

        JobCompletableFuture<Map<String, Long>> future1 = job1
                .mapper(new MyMapper())
                .combiner(new MyCombinerFactory())
                .reducer(new MyReducerFactory())
                .submit();
        JobCompletableFuture<Long> future2 = job2
                .mapper(new MyMapper())
                .combiner(new MyCombinerFactory())
                .reducer(new MyReducerFactory())
                .submit(new MyCollator());

        try {
            logger.debug("End MapReduce task");
            Long result2 = future2.get();
            Map<String, Long> result1 = future1.get();
            manualCnt[0] = 0;
            result1.entrySet().forEach(e -> manualCnt[0] += e.getValue());
            logger.debug("result1 after manualCnt = {}", manualCnt[0]);
            logger.debug("result2 = {}", result2);
        } catch (InterruptedException | ExecutionException e) {
            logger.debug("Error", e);
        }

        tracker.destroy();
        map.destroy();
    }

    private static void mapDistributedQueries(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap distributed queries --");

        IMap<String, Employee> employees = hz2.getMap("employees");
        employees.clear();
        employees.set("Alexander", new Employee(110));
        employees.set("Igor", new Employee(100));
        employees.set("Dima", new Employee(400));
        employees.set("Galya", new Employee(1000));
        employees.set("Kesha", new Employee(100));

        Predicate predicateEq = Predicates.equal("salary", 1000);
        logger.debug("result 'equal': {}", employees.values(predicateEq));
        Predicate predicateGte = Predicates.greaterEqual("salary", 400);
        logger.debug("result 'greaterEqual': {}", employees.values(predicateGte));

        Predicate less = Predicates.lessThan("salary", 110);
        Predicate greater = Predicates.greaterThan("salary", 400);
        Predicate predicateOr = Predicates.or(less, greater);
        logger.debug("result 'predicateOr': {}", employees.values(predicateOr));

        EntryObject entry = new PredicateBuilder().getEntryObject();
        PredicateBuilder equal = entry.get("salary").equal(400);
        logger.debug("builder find: {}", employees.values(equal));

        SqlPredicate sqlSalary = new SqlPredicate("salary < 110 or salary > 400");
        logger.debug("sqlSalary find: {}", employees.values(sqlSalary));
        SqlPredicate sqlPrepayment = new SqlPredicate("prepayment between 100 and 250");
        logger.debug("sqlPrepayment find: {}", employees.values(sqlPrepayment));

        employees.destroy();
    }

    private static void mapListeners(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap Listeners --");

        IMap<String, Employee> employees = hz2.getMap("employees");
        employees.addEntryListener(new MembersMapListener(), true);
        employees.addEntryListener(new LocalMapListener(), true);
        employees.addEntryListener(new MemberPredicateListener(), new SqlPredicate("salary=1000"), true);

        employees.set("Alexander", new Employee(100));
        employees.set("Igor", new Employee(100));
        employees.set("Dima", new Employee(100));
        employees.set("Galya", new Employee(1000));
        employees.set("Kesha", new Employee(100));
        employees.put("Galya", new Employee(2000));

        employees.destroy();
    }

    private static void mapEntryProcessor(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap EntryProcessor --");

        IMap<String, Employee> employees = hz2.getMap("employees");
        employees.set("Alexander", new Employee(100));
        employees.set("Igor", new Employee(100));
        employees.set("Dima", new Employee(100));
        employees.set("Galya", new Employee(1000));
        employees.set("Kesha", new Employee(100));
        employees.entrySet().forEach(t -> logger.debug("name: {}, salary: {}", t.getKey(), t.getValue().getSalary()));

        logger.debug("# Update salary for employees");
        employees.executeOnEntries(new RaiseSalaryTask(10));
        employees.entrySet().forEach(t -> logger.debug("name: {}, salary: {}", t.getKey(), t.getValue().getSalary()));

        logger.debug("# Calculate salary for employees");
        Map<String, Object> result = employees.executeOnEntries(new GetSalaryAmount());
        int sum = result.entrySet().stream()
                .mapToInt(e -> (Integer) e.getValue())
                .reduce(0, (a, b) -> a + b);
        logger.debug("salary sum: {}", sum);

        int threshold = 1000;
        logger.debug("# Delete employee with salary above: {}", threshold);
        employees.executeOnEntries(new DeleteEmployee(threshold));
        employees.entrySet().forEach(t -> logger.debug("name: {}, salary: {}", t.getKey(), t.getValue().getSalary()));
    }

    private static void mapDestroyLock(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap destroy lock --");

        ICountDownLatch start = hz2.getCountDownLatch("start");
        ICountDownLatch end = hz2.getCountDownLatch("end");
        start.trySetCount(1);
        end.trySetCount(1);
        int key = Integer.MAX_VALUE;
        IMap<Integer, LockVakue> mapLock1 = hz1.getMap("mapLock1");
        mapLock1.set(key, new LockVakue(key));
        Runnable lockTask = buildTryLockTask(hz2, key);

        service.execute(lockTask);
        logger.debug("1 try acquire lock on key: {}", key);
        mapLock1.lock(key);
        logger.debug("1 acquired lock on key: {}", key);
        start.countDown();
        try {
            int sleep = 3000;
            logger.debug("sleep: {} ms", sleep);
            Thread.sleep(sleep);
            logger.debug("1 try destroy map!");
            mapLock1.destroy();
            logger.debug("1 destroyed map!");
            end.await(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
        logger.debug("mapLock1 size: {}", mapLock1.size());
    }

    private static void mapReentrantLock(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap reentrant lock --");

        IMap<Integer, LockVakue> mapLock1 = hz1.getMap("mapLock1");
        int key = 5;
        logger.debug("try to lock 1st mapLock1 on key: {}", key);
        mapLock1.lock(key);
        logger.debug("key: {} is locked: {}", key, mapLock1.isLocked(key));

        logger.debug("try to lock 2nd mapLock1 on key: {}", key);
        mapLock1.lock(key);
        logger.debug("key: {} is locked: {}", key, mapLock1.isLocked(key));

        logger.debug("try to unlock mapLock1 on key: {}", key);
        mapLock1.unlock(key);
        mapLock1.unlock(key);

        mapLock1.destroy();
    }

    private static void mapLeaseLock(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap lease lock --");

        ICountDownLatch latch1 = hz2.getCountDownLatch("latch1");
        IMap<Integer, LockVakue> mapLock1 = hz1.getMap("mapLock1");
        final int key = 1;
        final int leaseTimeSec = 4;
        service.submit(() -> {
            logger.debug("1 try to acquire the lock ... key: {}", key);
            mapLock1.lock(key, leaseTimeSec, TimeUnit.SECONDS);
            latch1.countDown();
            logger.debug("1 acquire lock and hold {} sec", leaseTimeSec);
        });

        try {
            latch1.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
        logger.debug("2 try to acquire the lock ... key: {}", key);
        try {
            boolean lock = mapLock1.tryLock(key, leaseTimeSec + 2, TimeUnit.SECONDS, leaseTimeSec, TimeUnit.SECONDS);
            if (lock) {
                logger.debug("2 acquire the lock");
                mapLock1.unlock(key);
                logger.debug("2 release the lock");
            } else {
                logger.debug("2 can not acquire the lock!");
            }
        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
        mapLock1.destroy();
    }

    private static void mapLock(HazelcastInstance hz1, HazelcastInstance hz2, ExecutorService service) {
        logger.debug("-- IMap lock --");

        CountDownLatch start = new CountDownLatch(2);
        CountDownLatch end = new CountDownLatch(2);
        IMap<Integer, LockVakue> mapBackup1 = hz1.getMap("mapBackup1");
        Runnable task1 = buildLockTask(mapBackup1, "first", start, end);
        Runnable task2 = buildLockTask(mapBackup1, "two", start, end);
        service.execute(task1);
        service.execute(task2);
        try {
            start.await();
            end.await();
        } catch (InterruptedException e) {
            logger.error("Error", e);
        }
        logger.debug("mapBackup1 size: {}", mapBackup1.size());
        mapBackup1.entrySet().forEach(e -> logger.debug("key: {}, value: {}", e.getKey(), e.getValue()));
        mapBackup1.destroy();
    }

    private static void mapNearCash(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap near cash --");

        IMap<Integer, Integer> mapNearCash = hz2.getMap("mapNearCash");
        IntStream.range(0, 500).forEach(id -> mapNearCash.set(id, id));

        {
            NearCacheStats nearCacheStats = mapNearCash.getLocalMapStats().getNearCacheStats();
            logger.debug("mapNearCash size: {}", mapNearCash.size());
            logger.debug("mapNearCash misses: {}", nearCacheStats.getMisses());
            logger.debug("mapNearCash hits: {}", nearCacheStats.getHits());
            logger.debug("mapNearCash count: {}", nearCacheStats.getOwnedEntryCount());
            logger.debug("mapNearCash memory: {} bytes", nearCacheStats.getOwnedEntryMemoryCost());
        }
        Set<Integer> reads = new HashSet<>();
        logger.debug("read keys [100 .. 200]");
        IntStream.rangeClosed(1, 10000).forEach(t ->
                IntStream.rangeClosed(100, 120).forEach(id -> {
                    Integer value = mapNearCash.get(id);
                    reads.add(value);
                }));

        NearCacheStats nearCacheStats = mapNearCash.getLocalMapStats().getNearCacheStats();
        logger.debug("mapNearCash size: {}", mapNearCash.size());
        logger.debug("mapNearCash misses: {}", nearCacheStats.getMisses());
        logger.debug("mapNearCash hits: {}", nearCacheStats.getHits());
        logger.debug("mapNearCash count: {}", nearCacheStats.getOwnedEntryCount());
        logger.debug("mapNearCash memory: {} bytes", nearCacheStats.getOwnedEntryMemoryCost());
        logger.debug("Have reads keys: {}", reads);
        mapNearCash.destroy();
    }

    private static void mapBackup(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap backup --");
        IMap<Integer, Integer> mapBackup1Node1 = hz1.getMap("mapBackup1");
        IMap<Integer, Integer> mapBackup1Node2 = hz2.getMap("mapBackup1");
        IntStream.range(0, 500).forEach(id -> mapBackup1Node1.set(id, id));

        LocalMapStats localMapStats1 = mapBackup1Node1.getLocalMapStats();
        LocalMapStats localMapStats2 = mapBackup1Node2.getLocalMapStats();
        logger.debug("mapBackup1 size: {}", mapBackup1Node1.size());
        logger.debug("backupCount node1 : {}, node2: {}",
                localMapStats1.getBackupCount(), localMapStats2.getBackupCount());
        logger.debug("backupEntryCount node1 : {}, node2: {}",
                localMapStats1.getBackupEntryCount(), localMapStats2.getBackupEntryCount());
        logger.debug("backupMemory node1 : {} bytes, node2: {} bytes",
                localMapStats1.getBackupEntryMemoryCost(), localMapStats2.getBackupEntryMemoryCost());
        mapBackup1Node1.destroy();
        mapBackup1Node2.destroy();
    }

    private static void mapIdleTimeoutEviction(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap idle eviction --");

        IMap<Integer, Integer> mapEvict1Node1 = hz1.getMap("mapEvict1");
        for (int i = 0; i < 15; i++) {
            mapEvict1Node1.put(i, i);
        }
        logger.debug("mapEvict1Node2; Before Timeout size: {}", hz2.<Integer, Integer>getMap("mapEvict1").size());
        try {
            int millis = 10_000;
            logger.debug("Sleep ... {} ms", millis);
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.error("Error", e);
        }
        logger.debug("mapEvict1Node2; After Timeout should decrease size: {}", hz2.getMap("mapEvict1").size());
        mapEvict1Node1.destroy();
    }

    private static void mapMaxSizeEviction(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap max size eviction --");

        final int values = 271;
        Set<Integer> kyes = new HashSet<>();
        IMap<Integer, Integer> mapEvict2Node1 = hz1.getMap("mapEvict2");
        mapEvict2Node1.clear();
        for (int i = 0; i < values; i++) {
            kyes.add(i);
            mapEvict2Node1.put(i, i);
        }
        logger.debug("kyes: {}, mapEvict2Node1 size: {}", kyes.size(), hz1.<Integer, Integer>getMap("mapEvict2").size());
        kyes.forEach(k -> {
            if (!mapEvict2Node1.containsKey(k)) logger.debug("key: {}, mapEvict2Node1 haz key: {}", k, false);
        });
        logger.debug("Append new {} entries ...", values);
        IMap<Integer, Integer> mapEvict2Node2 = hz2.getMap("mapEvict2");
        for (int i = values; i < values * 2; i++) {
            kyes.add(i);
            mapEvict2Node2.put(i, i);
        }
        logger.debug("kyes: {}, mapEvict2Node2 size: {}, mapEvict2Node1 size: {}", kyes.size(),
                hz2.getMap("mapEvict2").size(), hz1.getMap("mapEvict2").size());
        mapEvict2Node1.destroy();
    }

    private static void mapPartitionAware(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap partition aware --");

        hz1.getMap("mapBin").destroy();
        hz1.getMap("mapObj").destroy();
        IMap<Integer, Customer> mapBin = hz1.getMap("mapBin");
        IMap<OrderKey, Order> mapObj = hz1.getMap("mapObj");
        IMap<Integer, Order> mapObj2 = hz1.getMap("mapObj2");

        Customer customer = new Customer(12);
        Order order = new Order(100, customer.getCustomerId());
        OrderKey orderKey = new OrderKey(order.getOrderId(), customer.getCustomerId());
        mapBin.set(customer.getCustomerId(), customer);
        mapObj.set(orderKey, order);
        mapObj2.set(order.getOrderId(), order);

        PartitionService pService = hz1.getPartitionService();
        Partition cPartition = pService.getPartition(customer.getCustomerId());
        Partition oPartition = pService.getPartition(orderKey);
        Partition wPartition = pService.getPartition(order.getOrderId());
        logger.debug("Partition for customer: {}", cPartition.getPartitionId());
        logger.debug("Partition for order with OrderKey: {}", oPartition.getPartitionId());
        logger.debug("Partition for order without OrderKey: {}", wPartition.getPartitionId());
    }

    private static void mapMemoryFormat(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap in memory format --");
        Map<Person, Object> mapJava = new HashMap<>();
        IMap<Person, Object> mapBin = hz1.getMap("mapBin");
        IMap<Person, Object> mapObj = hz1.getMap("mapObj");

        mapJava.put(new Person(DIMA, "Shishmakov"), true);
        mapBin.set(new Person(DIMA, "Shishmakov"), true);
        mapObj.set(new Person(DIMA, "Shishmakov"), true);

        logger.debug("mapJava; has: {}", mapJava.containsKey(new Person(DIMA, "Ivanov")));
        logger.debug("mapBin; has: {}", mapBin.containsKey(new Person(DIMA, "Ivanov")));
        logger.debug("mapObj; has: {}", mapObj.containsKey(new Person(DIMA, "Ivanov")));
    }

    private static void mapGetCopy(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- IMap get copy --");
        IMap<Integer, Name> map1Node1 = hz1.getMap("map1");
        map1Node1.set(1, new Name(DIMA));
        Name nameNode1 = map1Node1.get(1);
        nameNode1.setName(IGORE);

        IMap<Integer, Name> map1Node2 = hz2.getMap("map1");
        logger.debug("before set; nameNode2: {}", map1Node2.get(1).getName());
        assert DIMA.equals(map1Node2.get(1).getName()) : "map1 should have name: " + DIMA;

        map1Node1.set(1, nameNode1);
        map1Node2 = hz2.getMap("map1");
        logger.debug("after set; nameNode2: {}", map1Node2.get(1).getName());
        assert IGORE.equals(map1Node2.get(1).getName()) : "map1 should have name: " + IGORE;

    }

    public static class WordMapper implements Mapper<Integer, String, String, Long> {

        private final Long ONE_ENTRY = 1L;

        @Override
        public void map(Integer key, String value, Context<String, Long> context) {
            StringTokenizer tokenizer = new StringTokenizer(value);
            while (tokenizer.hasMoreTokens()) {
                String token = StringUtils.trimToEmpty(tokenizer.nextToken());
                context.emit(token, ONE_ENTRY);
            }
        }
    }

    public static class WordCombiner implements CombinerFactory<String, Long, Long> {

        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public Combiner<Long, Long> newCombiner(String key) {
            return new Combiner<Long, Long>() {

                private long count;

                @Override
                public void beginCombine() {
                    logger.debug("start combiner");
                }

                @Override
                public void finalizeCombine() {
                    logger.debug("stop combiner");
                }

                @Override
                public void combine(Long value) {
                    count += value;
                }

                @Override
                public void reset() {
                    this.count = 0;
                }

                @Override
                public Long finalizeChunk() {
                    return this.count;
                }
            };
        }
    }

    public static class WordReducer implements ReducerFactory<String, Long, Long> {

        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public Reducer<Long, Long> newReducer(String key) {
            return new Reducer<Long, Long>() {

                private long count;

                @Override
                public void beginReduce() {
                    logger.debug("start reducer");
                }

                @Override
                public void reduce(Long value) {
                    count += value;
                }

                @Override
                public Long finalizeReduce() {
                    final long result = this.count;
                    this.count = 0;
                    return result;
                }
            };
        }
    }

    public static class WordCollator implements Collator<Map.Entry<String, Long>, List<Map.Entry<String, Long>>> {
        @Override
        public List<Map.Entry<String, Long>> collate(Iterable<Map.Entry<String, Long>> values) {
            List<Map.Entry<String, Long>> list = new ArrayList<>();
            for (Map.Entry<String, Long> value : values) {
                list.add(value);
            }
            Collections.sort(list, (a, b) -> Long.compare(b.getValue(), a.getValue()));
            return list;
        }
    }


    public static class MyCollator implements Collator<Map.Entry<String, Long>, Long> {
        @Override
        public Long collate(Iterable<Map.Entry<String, Long>> values) {
            long sum = 0;
            for (Map.Entry<String, Long> entry : values) {
                sum += entry.getValue();
            }
            return sum;
        }
    }


    public static class MyCombinerFactory implements CombinerFactory<String, Long, Long> {
        @Override
        public Combiner<Long, Long> newCombiner(String key) {
            return new Combiner<Long, Long>() {
                private long counter;

                @Override
                public void combine(Long value) {
                    counter += value;
                }

                @Override
                public Long finalizeChunk() {
                    return this.counter;
                }

                @Override
                public void reset() {
                    counter = 0;
                }
            };
        }
    }

    public static class MyReducerFactory implements ReducerFactory<String, Long, Long> {
        @Override
        public Reducer<Long, Long> newReducer(String key) {
            return new Reducer<Long, Long>() {
                private long counter;

                @Override
                public void reduce(Long value) {
                    counter += value;
                }

                @Override
                public Long finalizeReduce() {
                    final long result = counter;
                    counter = 0;
                    return result;
                }
            };
        }
    }

    public static class MyMapper implements Mapper<Integer, Long, String, Long> {
        @Override
        public void map(Integer key, Long value, Context<String, Long> context) {
//            context.emit("all_value", value);
            context.emit(String.valueOf(key), value);
        }
    }

    public static class SalaryYear implements Serializable {
        private String email;
        private int year;
        private List<SalaryMonth> months;

        public SalaryYear() {
        }

        public SalaryYear(String email, int year, List<SalaryMonth> months) {
            this.email = email;
            this.year = year;
            this.months = months;
        }

        public int getAnnualSalary() {
            int sum = months.stream()
                    .map(SalaryMonth::getSalary)
                    .reduce(0, (a, b) -> a + b);
            logger.debug("email: {} sum: {}", email, sum);
            return sum;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public List<SalaryMonth> getMonths() {
            return months;
        }

        public void setMonths(List<SalaryMonth> months) {
            this.months = months;
        }
    }

    public static class SalaryMonth implements Serializable {
        private Month month;
        private int salary;

        public SalaryMonth() {
        }

        public SalaryMonth(Month month, int salary) {
            this.month = month;
            this.salary = salary;
        }

        public Month getMonth() {
            return month;
        }

        public void setMonth(Month month) {
            this.month = month;
        }

        public int getSalary() {
            return salary;
        }

        public void setSalary(int salary) {
            this.salary = salary;
        }
    }

    public static class Employee2 implements Serializable {
        private String firstName;
        private String lastName;

        public Employee2() {
        }

        public Employee2(String firstName, String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
    }

    public static class Employee implements Serializable {
        private final int prepayment;
        private int salary;

        public Employee(int salary) {
            this.salary = salary;
            this.prepayment = (int) (salary * .25);
        }

        public int getSalary() {
            return salary;
        }

        public int incSalary(int delta) {
            return this.salary += delta;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "prepayment=" + prepayment +
                    ", salary=" + salary +
                    '}';
        }

    }

    private static Runnable buildTryLockTask(HazelcastInstance hz2, int key) {
        IMap<Integer, LockVakue> mapLock1 = hz2.getMap("mapLock1");
        ICountDownLatch start = hz2.getCountDownLatch("start");
        ICountDownLatch end = hz2.getCountDownLatch("end");
        return () -> {
            try {
                start.await(1, TimeUnit.DAYS);
                do {
                    logger.debug("2 try acquire lock on key: {}", key);
                } while (!mapLock1.tryLock(key, 500, TimeUnit.MILLISECONDS));

                logger.debug("2 acquire lock on key: {}", key);
                mapLock1.unlock(key);
                logger.debug("2 release lock on key: {}", key);
            } catch (InterruptedException e) {
                logger.error("Error", e);
            }
            end.countDown();
        };
    }


    private static Runnable buildLockTask(IMap<Integer, LockVakue> map, String name,
                                          CountDownLatch start, CountDownLatch end) {
        return () -> {
            start.countDown();
            for (int i = 0; i < 10; i++) {
                map.lock(i);
                try {
                    map.computeIfAbsent(i, (value) -> {
                        logger.debug("Add value: {} {} callable", value, name);
                        return new LockVakue(value);
                    });
                } finally {
                    map.unlock(i);
                }
            }
            end.countDown();
        };
    }

    public static class Customer implements Serializable {
        private final int customerId;

        public Customer(int customerId) {
            this.customerId = customerId;
        }

        public int getCustomerId() {
            return customerId;
        }
    }

    public static class Order implements Serializable {
        private final int orderId;
        private final int customerId;

        public Order(int orderId, int customerId) {
            this.orderId = orderId;
            this.customerId = customerId;
        }

        public int getOrderId() {
            return orderId;
        }

        public int getCustomerId() {
            return customerId;
        }
    }

    public static class OrderKey implements Serializable, PartitionAware<Integer> {
        private final int orderId;
        private final int customerId;

        public OrderKey(int orderId, int customerId) {
            this.orderId = orderId;
            this.customerId = customerId;
        }

        @Override
        public Integer getPartitionKey() {
            return customerId;
        }
    }


    public static class Person implements Serializable {
        private final String name;
        private final String surname;

        public Person(String name, String surname) {
            this.name = name;
            this.surname = surname;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Person)) return false;
            Person person = (Person) o;
            return Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }


    public static class Name implements Serializable {

        private String name;

        public Name(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class LockVakue implements Serializable {
        private final int value;

        public LockVakue(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "LockVakue{" +
                    "value=" + value +
                    '}';
        }
    }

    public static class RaiseSalaryTask implements EntryProcessor<String, Employee> {

        private final int indexingRate;

        public RaiseSalaryTask(int indexingRate) {
            this.indexingRate = indexingRate;
        }

        @Override
        public Object process(Map.Entry<String, Employee> entry) {
            Employee employee = entry.getValue();
            employee.incSalary(indexingRate);
            entry.setValue(employee);
            return null;
        }

        @Override
        public EntryBackupProcessor<String, Employee> getBackupProcessor() {
            return null;
        }
    }

    private static class GetSalaryAmount extends AbstractEntryProcessor<String, Employee> {

        public GetSalaryAmount() {
            super(false);
        }

        @Override
        public Object process(Map.Entry<String, Employee> entry) {
            return entry.getValue().getSalary();
        }
    }

    private static class DeleteEmployee extends AbstractEntryProcessor<String, Employee> {

        private int threshold;

        public DeleteEmployee(int threshold) {
            super(false);
            this.threshold = threshold;
        }

        @Override
        public Object process(Map.Entry<String, Employee> entry) {
            if (entry.getValue().getSalary() > threshold) {
                entry.setValue(null);
            }
            return null;
        }
    }

    private static class MembersMapListener implements
            EntryAddedListener<String, Employee>,
            EntryUpdatedListener<String, Employee> {

        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public void entryAdded(EntryEvent<String, Employee> event) {
            logger.debug("Added: {}", event);
        }

        @Override
        public void entryUpdated(EntryEvent<String, Employee> event) {
            logger.debug("Updated: {}", event);
        }
    }

    private static class LocalMapListener implements
            EntryAddedListener<String, Employee>,
            EntryUpdatedListener<String, Employee> {

        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public void entryAdded(EntryEvent<String, Employee> event) {
            logger.debug("Added local: {}", event);
        }

        @Override
        public void entryUpdated(EntryEvent<String, Employee> event) {
            logger.debug("Updated local: {}", event);
        }
    }

    private static class MemberPredicateListener implements EntryAddedListener<String, Employee> {

        @Override
        public void entryAdded(EntryEvent<String, Employee> event) {
            logger.debug("With predicate added! : {}", event);
        }
    }
}
