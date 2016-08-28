package ru.shishmakov;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter8;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final String GROUP_NAME = "dev-hz-example";
    private static final String GROUP_PASSWORD = "dev-hz-example";

    private static ExecutorService service = Executors.newCachedThreadPool();


    public static void main(String[] args) throws InterruptedException {

        HazelcastInstance hz1 = buildHZInstance();
        HazelcastInstance hz2 = buildHZInstance();

        try {
//        Chapter2.doExamples(hz1, hz2);
//        Chapter3.doExamples(hz1, hz2, service);
//        Chapter4.doExamples(hz1, hz2, service);
//        Chapter5.doExamples(hz1, hz2, service);
//        Chapter6.doExamples(hz1, hz2, service);
//        Chapter7.doExamples(hz1, hz2, service);
            Chapter8.doExamples(hz1, hz2, service);
        } finally {
            service.shutdownNow();
            service.awaitTermination(15, TimeUnit.SECONDS);
            Hazelcast.shutdownAll();
        }
    }

    public static HazelcastInstance buildHZInstance() {
        logger.debug("Load HZ instance ...");
//        return loadFromClassPath();
        return loadFromFileDirectly();
//        return loadFromProgrammatically();
    }

    public static HazelcastInstance buildHZClientInstance() {
        logger.debug("Load HZ Client instance ...");
        return loadClientFromClassPath();
//        return loadClientFromFileDirectly();
//        return loadClientFromProgrammatically();
    }

    private static HazelcastInstance loadFromProgrammatically() {
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "slf4j");
        GroupConfig group = config.getGroupConfig();
        group.setName(GROUP_NAME);
        group.setPassword(GROUP_PASSWORD);
        NetworkConfig network = config.getNetworkConfig();
        network.setPortAutoIncrement(true);
        network.setPort(5701);
        network.getJoin().getMulticastConfig().setEnabled(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance loadClientFromProgrammatically() {
        ClientConfig config = buildClientConfig();
        return HazelcastClient.newHazelcastClient(config);
    }

    public static ClientConfig buildClientConfig() {
        ClientConfig config = new ClientConfig();
        config.setProperty("hazelcast.logging.type", "slf4j");

        GroupConfig group = config.getGroupConfig();
        group.setName(GROUP_NAME);
        group.setPassword(GROUP_PASSWORD);

        ClientNetworkConfig network = config.getNetworkConfig();
        network.addAddress("0.0.0.0");
        network.setSmartRouting(true);
        network.setConnectionTimeout(5000);
        network.setConnectionAttemptPeriod(3000);
        network.setConnectionAttemptLimit(10);

        SocketOptions so = network.getSocketOptions();
        so.setReuseAddress(true);
        so.setTcpNoDelay(true);
        return config;
    }

    private static HazelcastInstance loadFromClassPath() {
        return Hazelcast.newHazelcastInstance();
    }

    private static HazelcastInstance loadClientFromClassPath() {
        return HazelcastClient.newHazelcastClient();
    }

    private static HazelcastInstance loadFromFileDirectly() {
        Config config = new ClasspathXmlConfig("hazelcast.xml");
//        Config config = new FileSystemXmlConfig("hazelcast.xml");
//        Config config = new UrlXmlConfig("http://foo/hazelcast.xml");
        return Hazelcast.newHazelcastInstance(config);
    }

    private static HazelcastInstance loadClientFromFileDirectly() {
        try {
            XmlClientConfigBuilder xmlConfig = new XmlClientConfigBuilder("hazelcast-client.xml");
            return HazelcastClient.newHazelcastClient(xmlConfig.build());
        } catch (IOException e) {
            throw new IllegalStateException("Error with client config file", e);
        }
    }

}
