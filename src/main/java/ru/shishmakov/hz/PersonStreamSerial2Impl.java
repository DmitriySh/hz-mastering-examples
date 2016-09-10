package ru.shishmakov.hz;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9.PersonStreamSerial2;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.StreamSerializers.PERSON_SERIALIZER_2;

/**
 * Created by dima on 10.09.16.
 */
public class PersonStreamSerial2Impl implements StreamSerializer<PersonStreamSerial2> {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void write(ObjectDataOutput out, PersonStreamSerial2 person) throws IOException {
        out.writeObject(person.getStreamSerial());
        logger.debug("-->  write person: {}", person);
    }

    @Override
    public PersonStreamSerial2 read(ObjectDataInput in) throws IOException {
        PersonStreamSerial2 person = new PersonStreamSerial2(in.readObject());
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_SERIALIZER_2;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
