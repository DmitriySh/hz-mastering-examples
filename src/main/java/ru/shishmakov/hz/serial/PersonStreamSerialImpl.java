package ru.shishmakov.hz.serial;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9_Serialization.PersonStreamSerial;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * @author Dmitriy Shishmakov
 */
public class PersonStreamSerialImpl implements StreamSerializer<PersonStreamSerial> {

    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void write(ObjectDataOutput out, PersonStreamSerial person) throws IOException {
        out.writeUTF(person.getName());
        logger.debug("-->  write person: {}", person);
    }

    @Override
    public PersonStreamSerial read(ObjectDataInput in) throws IOException {
        PersonStreamSerial person = new PersonStreamSerial();
        person.setName(in.readUTF());
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return SerializerIds.PERSON_SERIALIZER;
    }

    @Override
    public void destroy() {
        logger.debug("x--  destroy serializer: {}", this);
    }
}
