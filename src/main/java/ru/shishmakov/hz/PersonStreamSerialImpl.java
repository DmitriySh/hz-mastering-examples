package ru.shishmakov.hz;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.ch.Chapter9.PersonStreamSerial;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import static ru.shishmakov.hz.StreamSerializerConst.PERSON_SERIALIZER;

/**
 * Created by dima on 10.09.16.
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
        String name = in.readUTF();
        PersonStreamSerial person = new PersonStreamSerial();
        person.setName(name);
        logger.debug("<--  read person: {}", person);
        return person;
    }

    @Override
    public int getTypeId() {
        return PERSON_SERIALIZER;
    }

    @Override
    public void destroy() {

    }
}
