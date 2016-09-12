package ru.shishmakov.hz.serial;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import de.javakaffee.kryoserializers.*;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationHandler;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static ru.shishmakov.hz.serial.SerializerIds.SMART_KRYO_SERIALIZER;

/**
 * Created by dima on 11.09.16.
 */
public class KryoSmartStreamSerialImpl<T> implements StreamSerializer<T> {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    private static final int MAX_CAPACITY = 4096;
    private static final Set<Class<?>> classes = new HashSet<>();

    private static final ThreadLocal<Kryo> kryoLocal = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

            // register JDK classes
            UnmodifiableCollectionsSerializer.registerSerializers(kryo);
            SynchronizedCollectionsSerializer.registerSerializers(kryo);
            kryo.register(InvocationHandler.class, new JdkProxySerializer());
            kryo.register(EnumMap.class, new EnumMapSerializer());
            kryo.register(EnumSet.class, new EnumSetSerializer());
            // ... and many others serializers from package 'de.javakaffee'

            // register user classes
            classes.forEach(kryo::register);
            return kryo;
        }
    };

    public KryoSmartStreamSerialImpl() {
    }

    public KryoSmartStreamSerialImpl(Set<Class<?>> userClasses) {
        userClasses.forEach(cl -> {
            if (!classes.contains(cl)) classes.add(cl);
        });
    }

    @Override
    public void write(ObjectDataOutput out, T object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(MAX_CAPACITY);
        Output output = new Output(new byte[MAX_CAPACITY]);
        output.setOutputStream(baos);
        kryoLocal.get().writeClassAndObject(output, object);
        output.flush();

        out.writeInt(baos.size());
        baos.writeTo((OutputStream) out);
        logger.debug("-->  write object: {}", object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        byte[] buffer = new byte[size];
        int read = ((InputStream) in).read(buffer, 0, size);
        checkState(read == size, "object expect: %s bytes, real: %s bytes", size, read);

        T object = (T) kryoLocal.get().readClassAndObject(new Input(buffer));
        logger.debug("<--  read object: {}", object);
        return object;
    }

    @Override
    public int getTypeId() {
        return SMART_KRYO_SERIALIZER;
    }

    @Override
    public void destroy() {
        kryoLocal.remove();
        logger.debug("x--  destroy serializer: {}", this);
    }
}
