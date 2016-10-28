package ru.shishmakov.hz.spi;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * During the execution of a migration, no other operations will be running in that partition.
 * Therefore, you don’t need to deal with thread-safety.
 *
 * @author Dmitriy Shishmakov on 24.10.16
 */
public class MigrationOperation extends AbstractOperation {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private Map<String, Integer> migrationData = Collections.emptyMap();

    public MigrationOperation() {
        /* need to be */
    }

    public MigrationOperation(Map<String, Integer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CounterService service = getService();
        CounterContainer container = service.getContainerByPartitionId(getPartitionId());
        container.applyMigrationData(migrationData);
        logger.debug("Apply migration value: {}, address: {}", migrationData, getNodeEngine().getThisAddress());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        migrationData.forEach((k, v) -> {
            try {
                out.writeUTF(k);
                out.writeInt(v);
            } catch (IOException e) {
                logger.error("Error", e);
            }
        });
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<String, Integer> map = new HashMap<>();
        IntStream.range(0, size).forEach(t -> {
            try {
                map.put(in.readUTF(), in.readInt());
            } catch (IOException e) {
                logger.debug("Error", e);
            }
        });
        migrationData = map;

    }
}
