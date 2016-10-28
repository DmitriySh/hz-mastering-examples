package ru.shishmakov.hz.spi;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.spi.CounterService.NAME;

/**
 * Hazelcast does remote call through a Proxy - on the client side, you get a proxy that exposes your methods.
 * It does not contain counter state; it is just a local representative of remote data/functionality.
 *
 * @author Dmitriy Shishmakov on 19.10.16
 */
public class CounterProxy extends AbstractDistributedObject<CounterService> implements Counter {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    static final String CLASS_NAME = CounterProxy.class.getSimpleName();

    private final String objectId;

    public CounterProxy(String objectId, NodeEngine nodeEngine, CounterService service) {
        super(nodeEngine, service);
        this.objectId = objectId;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getServiceName() {
        return objectId;
    }

    /**
     * Method is sending operations to the correct machine, executing the operation, and returning the results.
     * Action needs to be invoked on the machine for hosting the partition that contains the real counter.
     *
     * @return new value
     */
    @Override
    public int increment(int delta) {
        NodeEngine engine = getNodeEngine();
        final int partitionId = engine.getPartitionService().getPartitionId(objectId);
        IncOperation operation = new IncOperation(objectId, delta);

        InvocationBuilder builder = engine.getOperationService().createInvocationBuilder(NAME, operation, partitionId);
        ICompletableFuture<Integer> future = builder.invoke();
        try {
            return future.get();
        } catch (Exception e) {
            logger.error("Error", e);
//            throw Throwables.propagate(e);
            throw ExceptionUtil.rethrow(e);
        }
    }
}
