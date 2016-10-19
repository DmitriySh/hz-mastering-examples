package ru.shishmakov.hz.spi;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Properties;

/**
 * @author Dmitriy Shishmakov on 19.10.16
 */
public class CounterService implements ManagedService, RemoteService {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String NAME = "CounterService";
    private static final String CLASS_NAME = CounterService.class.getSimpleName();

    private NodeEngine nodeEngine;
    private Properties properties;

    public CounterService() {
        /* need to be */
        System.out.println("CounterService");
    }

    /**
     * CounterService is initialized
     */
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.properties = properties;
        System.out.println("{} init");
        logger.debug("{} init", CLASS_NAME);
    }

    /**
     * Clean up resources
     */
    @Override
    public void shutdown(boolean terminate) {
        logger.debug("{} shutdown", CLASS_NAME);
    }

    /**
     * Merge after the split-brain problem
     */
    @Override
    public void reset() {
        /* not implement */
        logger.debug("{} reset", CLASS_NAME);
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        logger.debug("{} create proxy: {}", CLASS_NAME, CounterProxy.class.getSimpleName());
        return new CounterProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        /* not implement */
        logger.debug("{} destroy proxy", CLASS_NAME);
    }
}
