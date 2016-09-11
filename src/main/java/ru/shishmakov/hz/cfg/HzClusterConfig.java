package ru.shishmakov.hz.cfg;

import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.Main.GROUP_NAME;
import static ru.shishmakov.Main.GROUP_PASSWORD;

/**
 * Created by dima on 02.09.16.
 */
public class HzClusterConfig {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static HazelcastInstance buildHZInstance() {
        logger.debug("Load HZ instance ...");
//        return loadFromClassPath();
        return loadFromFileDirectly();
//        return loadFromProgrammatically();
    }

    private static HazelcastInstance loadFromProgrammatically() {
        Config config = buildClusterConfig();
        return Hazelcast.newHazelcastInstance(config);
    }

    public static Config buildClusterConfig() {
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "slf4j");
        GroupConfig group = config.getGroupConfig();
        group.setName(GROUP_NAME);
        group.setPassword(GROUP_PASSWORD);
        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(true);
        network.setPort(5701);
        network.getJoin().getMulticastConfig().setEnabled(true);
        return config;
    }

    private static HazelcastInstance loadFromClassPath() {
        return Hazelcast.newHazelcastInstance();
    }

    private static HazelcastInstance loadFromFileDirectly() {
        try {
//            Config config = new ClasspathXmlConfig("hazelcast.xml");
            Config config = new FileSystemXmlConfig("src/main/resources/hazelcast.xml");
//            Config config = new UrlXmlConfig("http://foo/hazelcast.xml");
            return Hazelcast.newHazelcastInstance(config);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Could not load config file", e);
        }
    }
}
