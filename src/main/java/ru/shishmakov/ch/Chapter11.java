package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Dmitriy Shishmakov on 20.09.16
 */
public class Chapter11 {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.info("-- Chapter 11. JCache Provider --");

//        simpleCachingProvider();
        useCacheLoaderWriterListener();
    }

    private static void useCacheLoaderWriterListener() {
        logger.info("-- HZ loader and writer JCache --");

        Map<String, Integer> store = new HashMap<>();
        store.put("Monday", 1);
        store.put("Tuesday", 2);
        store.put("Wednesday", 3);
        store.put("Thursday", 4);
        store.put("Friday", 5);
        store.put("Saturday", 6);
        store.put("Sunday", 7);

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

//        CachingProvider provider = Caching.getCachingProvider("com.hazelcast.cache.HazelcastCachingProvider");
        try (CachingProvider provider = Caching.getCachingProvider()) {
            CacheManager cacheManager = provider.getCacheManager();
            Configuration<String, Integer> configuration = new MutableConfiguration<String, Integer>()
                    .setTypes(String.class, Integer.class);
            Cache<String, Integer> cache = cacheManager.createCache("cache2", configuration);

            cache.put("Monday", 1);
            cache.put("Tuesday", 2);
            cache.put("Wednesday", 3);
            cache.put("Thursday", 4);
            cache.put("Friday", 5);
            cache.put("Saturday", 6);
            cache.put("Sunday", 7);

            cache = cacheManager.getCache("cache2", String.class, Integer.class);

            logger.debug("cache: {}", cache);
            logger.debug("cache friday: {}", cache.get("Friday"));
        }
    }


    public static class XmlStoreCacheLoader extends StoreCacheLoader implements Factory<StoreCacheLoader> {
        private static final long serialVersionUID = 1L;

        @Override
        public StoreCacheLoader create() {
            return new XmlStoreCacheLoader();
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

    public static class XmlStoreCacheWriter extends StoreCacheWriter implements Factory<StoreCacheWriter> {
        private static final long serialVersionUID = 1L;

        @Override
        public StoreCacheWriter create() {
            return new XmlStoreCacheWriter();
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
