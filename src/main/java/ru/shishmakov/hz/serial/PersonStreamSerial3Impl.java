package ru.shishmakov.hz.serial;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9_Serialization.PersonStreamSerial3;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov
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
        return SerializerIds.PERSON_SERIALIZER_3;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
