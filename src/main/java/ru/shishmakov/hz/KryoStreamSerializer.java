package ru.shishmakov.hz;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import ru.shishmakov.ch.Chapter9.PersonStreamSerial;

import java.io.IOException;

/**
 * Created by dima on 10.09.16.
 */
public class KryoStreamSerializer implements StreamSerializer<PersonStreamSerial> {

    @Override
    public void write(ObjectDataOutput out, PersonStreamSerial object) throws IOException {

    }

    @Override
    public PersonStreamSerial read(ObjectDataInput in) throws IOException {
        return null;
    }

    @Override
    public int getTypeId() {
        return 0;
    }

    @Override
    public void destroy() {

    }
}
