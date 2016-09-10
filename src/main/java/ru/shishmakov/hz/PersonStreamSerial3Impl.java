package ru.shishmakov.hz;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9.PersonStreamSerial3;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.StreamSerializers.PERSON_SERIALIZER_3;

/**
 * Created by dima on 10.09.16.
 */
public class PersonStreamSerial3Impl implements StreamSerializer<PersonStreamSerial3> {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void write(ObjectDataOutput out, PersonStreamSerial3 person) throws IOException {
        out.writeObject(person.getMap());
        out.writeUTF(person.getName());
        logger.debug("-->  write person: {}", person);
    }

    @Override
    public PersonStreamSerial3 read(ObjectDataInput in) throws IOException {
        PersonStreamSerial3 person = new PersonStreamSerial3();
        person.setMap(in.readObject());
        person.setName(in.readUTF());
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_SERIALIZER_3;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
