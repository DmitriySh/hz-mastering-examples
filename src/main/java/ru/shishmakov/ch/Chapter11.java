package ru.shishmakov.ch;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov on 20.09.16
 */
public class Chapter11 {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- Chapter 11. JCache Provider --");

        simpleCachingProvider(hz1, hz2);
    }

    private static void simpleCachingProvider(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ simple JCache --");

        CachingProvider cachingProvider = Caching.getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        Configuration<String, Integer> configuration =
                new MutableConfiguration<String, Integer>().setTypes(String.class, Integer.class);
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

        cachingProvider.close();
    }
}
