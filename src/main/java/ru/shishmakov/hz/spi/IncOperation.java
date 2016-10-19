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
    private int amount;
    private int value;

    IncOperation(String objectId, int amount) {
        this.objectId = objectId;
        this.amount = amount;
    }

    @Override
    public void run() throws Exception {
        logger.debug("Execute increment on: {}, address: {}", objectId, getNodeEngine().getThisAddress());
        value = 0;
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
        out.writeInt(amount);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        amount = in.readInt();
    }
}
