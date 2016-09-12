package ru.shishmakov.hz.serial;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by dima on 10.09.16.
 */
public class MapStreamSerialImpl implements StreamSerializer<Map<?, ?>> {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void write(ObjectDataOutput out, Map<?, ?> map) throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<?, ?> e : map.entrySet()) {
            out.writeObject(e.getKey());
            out.writeObject(e.getValue());
        }
        logger.debug("-->  write map: {}", map);
    }

    @Override
    public Map<?, ?> read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        HashMap<?, ?> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(in.readObject(), in.readObject());
        }
        logger.debug("<--  read map: {}", map);
        return map;
    }

    @Override
    public int getTypeId() {
        return SerializerIds.MAP_SERIALIZER;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
