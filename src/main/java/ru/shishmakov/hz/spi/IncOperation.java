package ru.shishmakov.hz.spi;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov on 19.10.16
 */
class IncOperation extends AbstractOperation implements PartitionAwareOperation {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private String objectId;
    private int delta;
    private int value;

    public IncOperation() {
        /* need to be */
    }

    IncOperation(String objectId, int delta) {
        this.objectId = objectId;
        this.delta = delta;
    }

    @Override
    public void run() throws Exception {
        CounterService service = getService();
        CounterContainer container = service.getContainerByPartitionId(getPartitionId());
        container.increment(objectId, delta);
        value = container.getCount(objectId);
        logger.debug("Execute increment on: {}, value: {}, address: {}", objectId, value, getNodeEngine().getThisAddress());
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return value;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeInt(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        delta = in.readInt();
    }
}
