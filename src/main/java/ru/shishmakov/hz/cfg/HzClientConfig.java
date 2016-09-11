package ru.shishmakov.hz.cfg;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.Main.GROUP_NAME;
import static ru.shishmakov.Main.GROUP_PASSWORD;

/**
 * Created by dima on 02.09.16.
 */
public class HzClientConfig {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static HazelcastInstance buildHZClientInstance() {
        logger.debug("Load HZ Client instance ...");
        return loadClientFromClassPath();
//        return loadClientFromFileDirectly();
//        return loadClientFromProgrammatically();
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

    private static HazelcastInstance loadClientFromClassPath() {
        return HazelcastClient.newHazelcastClient();
    }

    private static HazelcastInstance loadClientFromFileDirectly() {
        try {
            XmlClientConfigBuilder xmlConfig = new XmlClientConfigBuilder("hazelcast-client.xml");
            return HazelcastClient.newHazelcastClient(xmlConfig.build());
        } catch (IOException e) {
            throw new IllegalStateException("Could not load client config file", e);
        }
    }

}
