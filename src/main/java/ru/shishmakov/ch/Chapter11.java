package ru.shishmakov.ch;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.*;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Dmitriy Shishmakov on 20.09.16
 */
public class Chapter11 {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final Map<String, Integer> store = new HashMap<>();

    static {
        store.put("Monday", 1);
        store.put("Tuesday", 2);
        store.put("Wednesday", 3);
        store.put("Thursday", 4);
        store.put("Friday", 5);
        store.put("Saturday", 6);
        store.put("Sunday", 7);
    }

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.info("-- Chapter 11. JCache Provider --");

//        simpleCachingProvider();
//        useProgramAndXmlCacheLoaderWriterListener();
//        useCacheEntryProcessor();
//        useCacheEntryListener();
//        useICacheAsyncMethods();
        useICacheExpiryPolicy();
    }

    private static void useICacheExpiryPolicy() {
        logger.info("-- HZ ICache expiry policy --");

        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Cache<String, Integer> cache = cacheManager.getCache("cache", String.class, Integer.class);

            @SuppressWarnings("unchecked")
            ICache<String, Integer> iCache = cache.unwrap(ICache.class);

            String monday = "Monday";
            logger.debug("add key: {} and sleep ...", monday);
            iCache.put(monday, 1, new AccessedExpiryPolicy(Duration.ONE_DAY));
            TimeUnit.SECONDS.sleep(3);
            logger.debug("contains key: {}={}", monday, iCache.containsKey(monday));

            String tuesday = "Tuesday";
            logger.debug("add key: {} and sleep ...", tuesday);
            iCache.put(tuesday, 1, new AccessedExpiryPolicy(new Duration(SECONDS, 3)));
            TimeUnit.SECONDS.sleep(5);
            logger.debug("contains key: {}={}", tuesday, iCache.containsKey(tuesday));


        } catch (InterruptedException e) {
            logger.debug("Error", e);
        }
    }

    private static void useICacheAsyncMethods() {
        logger.info("-- HZ ICache async methods --");

        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Cache<String, Integer> cache = cacheManager.getCache("cache", String.class, Integer.class);

            CountDownLatch latch = new CountDownLatch(1);

            @SuppressWarnings("unchecked")
            ICache<String, Integer> iCache = cache.unwrap(ICache.class);
            ICompletableFuture<Void> future = iCache.putAsync("Monday", 1);
            future.andThen(new ExecutionCallback<Void>() {
                @Override
                public void onResponse(Void response) {
                    logger.debug("Async onResponse 1: {}", response);
                    iCache.putIfAbsentAsync("Tuesday", 2).andThen(new ExecutionCallback<Boolean>() {
                        @Override
                        public void onResponse(Boolean response) {
                            logger.debug("Async onResponse 2: {}", response);
                            iCache.getAsync("Monday").andThen(new ExecutionCallback<Integer>() {
                                @Override
                                public void onResponse(Integer response) {
                                    logger.debug("Async onResponse 3: {}", response);
                                    logger.debug("Async methods has got response value: {}", response);
                                    latch.countDown();
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    logger.debug("Error in async 3", t);
                                    latch.countDown();
                                }
                            });
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            logger.debug("Error in async 2", t);
                            latch.countDown();
                        }
                    });
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.debug("Error in async 1", t);
                    latch.countDown();
                }
            });
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.debug("Error", e);
            }
        }
    }

    private static void useCacheEntryListener() {
        logger.info("-- HZ JCache EntryListener --");

        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Cache<String, Integer> cache = cacheManager.getCache("cacheEntryListener", String.class, Integer.class);

            int timeout = 10_000;
            cache.putAll(store);
            cache.put("Wednesday", 300);
            cache.remove("Thursday");

            logger.debug("Sleep in {} ms for expiration ...", timeout);
            Thread.sleep(timeout);

            cache.getAll(store.keySet());
        } catch (InterruptedException e) {
            logger.debug("error: {}", e, 300);
        }
    }

    private static void useCacheEntryProcessor() {
        logger.info("-- HZ JCache EntryProcessor --");

        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Cache<String, Integer> cache = cacheManager.getCache("cache", String.class, Integer.class);

            cache.putAll(store);
            cache.invokeAll(store.keySet(), new MultiplierCacheProcessor(5), 10);
        }
    }

    private static void useProgramAndXmlCacheLoaderWriterListener() {
        logger.info("-- HZ backing JCache CacheLoader and CacheWriter --");

//        CachingProvider provider = Caching.getCachingProvider("com.hazelcast.cache.HazelcastCachingProvider")
        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            MutableConfiguration<String, Integer> configuration = new MutableConfiguration<String, Integer>()
                    .setTypes(String.class, Integer.class)
                    .setReadThrough(true).setWriteThrough(true)
                    .setCacheLoaderFactory(FactoryBuilder.factoryOf(StoreCacheLoader.class))
                    .setCacheWriterFactory(FactoryBuilder.factoryOf(StoreCacheWriter.class))
                    .setManagementEnabled(false).setStatisticsEnabled(true);
            Cache<String, Integer> programmableCache = cacheManager.createCache("programCacheLoaderWriter", configuration);
            Cache<String, Integer> xmlCache = cacheManager.getCache("xmlCacheLoaderWriter", String.class, Integer.class);

            logger.debug("programmable cache");
            programmableCache.putAll(store);
            programmableCache.get("Wednesday");
            programmableCache.put("Wednesday", 300);
            programmableCache.remove("Thursday");
            programmableCache.removeAll();
            programmableCache.get("Monday");

            logger.debug("xml cache");
            xmlCache.putAll(store);
            xmlCache.get("Wednesday");
            xmlCache.put("Wednesday", 300);
            xmlCache.remove("Thursday");
            xmlCache.removeAll();
            xmlCache.get("Monday");
        }
    }

    private static void simpleCachingProvider() {
        logger.info("-- HZ simple JCache --");

//        try (CachingProvider provider = Caching.getCachingProvider("com.hazelcast.client.cache.impl.HazelcastClientCachingProvider")) {
//        try (CachingProvider provider = Caching.getCachingProvider("com.hazelcast.cache.impl.HazelcastServerCachingProvider")) {
//        try (CachingProvider provider = Caching.getCachingProvider("com.hazelcast.cache.HazelcastCachingProvider")) {
        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Cache<String, Integer> cache = cacheManager.getCache("cache", String.class, Integer.class);

            Map<Object, Object> temp = new HashMap<>();
            cache.forEach(e -> temp.put(e.getKey(), e.getValue()));
            logger.debug("cache before: {}", temp);
            cache.put("Monday", 1);
            cache.put("Tuesday", 2);
            cache.put("Wednesday", 3);
            cache.put("Thursday", 4);
            cache.put("Friday", 5);
            cache.put("Saturday", 6);
            cache.put("Sunday", 7);

            cache = cacheManager.getCache("cache2", String.class, Integer.class);
            temp.clear();
            cache.forEach(e -> temp.put(e.getKey(), e.getValue()));
            logger.debug("cache after: {}", temp);
            logger.debug("cache friday: {}", cache.get("Friday"));
        }
    }

    public static class CacheEntryListenerFactory implements Factory<CacheEntryListener> {
        private static final long serialVersionUID = 1L;

        @Override
        public CacheEntryListener create() {
            return new CacheEntryListener();
        }
    }

    public static class CacheEntryListener implements
            CacheEntryCreatedListener<String, Integer>,
            CacheEntryUpdatedListener<String, Integer>,
            CacheEntryRemovedListener<String, Integer>,
            CacheEntryExpiredListener<String, Integer> {

        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> cacheEntryEvents) throws CacheEntryListenerException {
            Map<String, Integer> map = new HashMap<>();
            cacheEntryEvents.forEach(e -> map.put(e.getKey(), e.getValue()));
            logger.debug("create entry: {}", map);
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> cacheEntryEvents) throws CacheEntryListenerException {
            Map<String, Integer> map = new HashMap<>();
            cacheEntryEvents.forEach(e -> map.put(e.getKey(), e.getValue()));
            logger.debug("remove entry: {}", map);
        }

        @Override
        public void onExpired(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> cacheEntryEvents) throws CacheEntryListenerException {
            Map<String, Integer> map = new HashMap<>();
            cacheEntryEvents.forEach(e -> map.put(e.getKey(), e.getValue()));
            logger.debug("expire entry: {}", map);
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends Integer>> cacheEntryEvents) throws CacheEntryListenerException {
            List<CacheEntryEvent> list = new ArrayList<>();
            cacheEntryEvents.forEach(list::add);
            logger.debug("update entry: {}", list);
        }
    }

    public static class MultiplierCacheProcessor implements EntryProcessor<String, Integer, Void>, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
        private static final long serialVersionUID = 1L;
        private final Integer threshold;

        public MultiplierCacheProcessor(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public Void process(MutableEntry<String, Integer> entry, Object... arguments) throws EntryProcessorException {
            int newValue = (int) arguments[0];
            Integer oldValue = entry.getValue();
            if (oldValue > threshold) {
                entry.setValue(newValue);
                logger.debug("oldValue: {}, newValue: {}", oldValue, newValue);
            }
            return null;
        }
    }

    public static class XmlStoreCacheLoader extends StoreCacheLoader implements Factory<StoreCacheLoader> {
        private static final long serialVersionUID = 1L;

        @Override
        public StoreCacheLoader create() {
            return new XmlStoreCacheLoader();
        }
    }

    public static class XmlStoreCacheWriter extends StoreCacheWriter implements Factory<StoreCacheWriter> {
        private static final long serialVersionUID = 1L;

        @Override
        public StoreCacheWriter create() {
            return new XmlStoreCacheWriter();
        }
    }

    public static class StoreCacheLoader implements CacheLoader<String, Integer>, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

        private static final long serialVersionUID = 1L;

        @Override
        public Integer load(String key) throws CacheLoaderException {
            logger.debug("load key: {}", key);
            return null;
        }

        @Override
        public Map<String, Integer> loadAll(Iterable<? extends String> keys) throws CacheLoaderException {
            logger.debug("load keys: {}", keys);
            return null;
        }

    }

    public static class StoreCacheWriter implements CacheWriter<String, Integer>, Serializable {
        private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
        private static final long serialVersionUID = 1L;

        @Override
        public void write(Cache.Entry<? extends String, ? extends Integer> entry) throws CacheWriterException {
            logger.debug("write entry: {}={}", entry.getKey(), entry.getValue());
        }

        @Override
        public void writeAll(Collection<Cache.Entry<? extends String, ? extends Integer>> entries) throws CacheWriterException {
            logger.debug("write all entries: {}", entries);
        }

        @Override
        public void delete(Object key) throws CacheWriterException {
            logger.debug("delete key: {}", key);
        }

        @Override
        public void deleteAll(Collection<?> keys) throws CacheWriterException {
            logger.debug("delete all keys: {}", keys);
        }
    }
}
