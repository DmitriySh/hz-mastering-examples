package ru.shishmakov.hz;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9.KryoPersonStreamSerial;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.StreamSerializers.PERSON_KRYO_SERIALIZER;

/**
 * Created by dima on 10.09.16.
 */
public class KryoPersonStreamSerialImpl implements StreamSerializer<KryoPersonStreamSerial> {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final ThreadLocal<Kryo> kryoLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register(KryoPersonStreamSerial.class);
            return kryo;
        }
    };

    @Override
    public void write(ObjectDataOutput out, KryoPersonStreamSerial person) throws IOException {
        Kryo kryo = KryoPersonStreamSerialImpl.kryoLocal.get();
        Output output = new Output((OutputStream) out);
        kryo.writeObject(output, person);
        output.flush();
        logger.debug("-->  write person: {}", person);
    }

    @Override
    public KryoPersonStreamSerial read(ObjectDataInput in) throws IOException {
        Kryo kryo = kryoLocal.get();
        Input input = new Input((InputStream) in);
        KryoPersonStreamSerial person = kryo.readObject(input, KryoPersonStreamSerial.class);
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_KRYO_SERIALIZER;
    }

    @Override
    public void destroy() {
        kryoLocal.remove();
        logger.debug("x--  destroy serializer: {}", this);
    }
}
