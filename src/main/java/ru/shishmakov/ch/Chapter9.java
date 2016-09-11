package ru.shishmakov.ch;

import com.google.common.reflect.ClassPath;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.shishmakov.hz.serial.KryoSmartStreamSerialImpl;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static ru.shishmakov.hz.cfg.HzClusterConfig.buildClusterConfig;
import static ru.shishmakov.hz.serial.DataSerializableImpl.*;
import static ru.shishmakov.hz.serial.PortableSerializableImpl.*;

/**
 * Created by dima on 02.09.16.
 */
public class Chapter9 {
    private static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void doExamples(HazelcastInstance hz1, HazelcastInstance hz2) {
//        useSerialization(hz1, hz2);
//        useExternalization(hz1, hz2);
//        useDataSerializable(hz1, hz2);
//        useIdentifiedDataSerializable(hz1, hz2);
//        usePortable(hz1, hz2);
//        writePortableField(hz1, hz2);
//        useStreamSerializer(hz1, hz2);
//        writeStreamSerializerField(hz1, hz2);
//        writeMapStreamSerializer(hz1, hz2);
//        useSimpleKryoStreamSerializer(hz1, hz2);
        useSmartKryoStreamSerializer();
    }


    private static void useSmartKryoStreamSerializer() {
        logger.debug("-- HZ smart Kryo StreamSerializer --");

        HazelcastInstance hz1 = initHzCustomNode();
        processKeyMap(hz1, new KryoPersonStreamSerial("Dmitriy", "Shishmakov", "History"));
        hz1.getCluster().shutdown();
    }

    private static void useSimpleKryoStreamSerializer(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ simple Kryo StreamSerializer --");

        processKeyMap(hz1, new KryoPersonStreamSerial("Dmitriy", "Shishmakov", "History"));
    }

    private static void writeMapStreamSerializer(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Map StreamSerializer --");

        Map<String, Integer> map = new MapStreamSerial<>();
        map.put("pen", 1);
        map.put("pencil", 1);
        processKeyMap(hz1, new PersonStreamSerial3(map, "Dmitriy", "Shishmakov", "History"));
    }

    private static void writeStreamSerializerField(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ StreamSerializer field --");

        processKeyMap(hz1, new PersonStreamSerial2(new PersonStreamSerial("Dmitriy", "Shishmakov", "History")));
    }

    private static void useStreamSerializer(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ StreamSerializer --");

        processKeyMap(hz1, new PersonStreamSerial("Dmitriy", "Shishmakov", "History"));
    }

    private static void writePortableField(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ write Portable field --");

        processKeyMap(hz1, new PersonPortable3(new PersonPortable2("Igor", "Alexandrovich", "Shishmakov", "History")));
    }

    private static void usePortable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Portable --");

        processKeyMap(hz1, new PersonPortable1("Dmitriy", "Alexandrovich", "Shishmakov", "History"));
        processKeyMap(hz1, new PersonPortable2("Igor", "Alexandrovich", "Shishmakov", "History"));
    }

    private static void useIdentifiedDataSerializable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ IdentifiedDataSerializable --");

        processKeyMap(hz1, new PersonIdentDataSerial1("Dmitriy", "Shishmakov", "History"));
        processKeyMap(hz1, new PersonIdentDataSerial2("Igor", "Shishmakov", "History"));
    }

    private static void useDataSerializable(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ DataSerializable --");

        processKeyMap(hz1, new PersonDataSerial("Dmitriy", "Shishmakov", "History"));
    }

    private static void useExternalization(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Externalizable --");

        processKeyMap(hz1, new PersonExternal("Dmitriy", "Shishmakov", "History"));
    }

    private static void useSerialization(HazelcastInstance hz1, HazelcastInstance hz2) {
        logger.debug("-- HZ Serializable --");

        processKeyMap(hz1, new PersonSerial("Dmitriy", "Alexandrovich", "Shishmakov", "History"));
    }

    private static HazelcastInstance initHzCustomNode() {
        Config config = buildClusterConfig();
        GroupConfig group = config.getGroupConfig();
        group.setName("dev-hz-kryo");
        group.setPassword("dev-hz-kryo");
        fillSerializationConfig(config.getSerializationConfig());
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void fillSerializationConfig(SerializationConfig serialCfg) {
        try {
            String packageName = Chapter9.class.getPackage().getName();
            ClassPath cp = ClassPath.from(Thread.currentThread().getContextClassLoader());
            Set<Class<?>> classes = cp.getAllClasses().stream()
                    .filter(cl -> cl.getName().contains("$"))
                    .filter(cl -> packageName.equalsIgnoreCase(cl.getPackageName()))
                    .map(cl -> {
                        try {
                            return Class.forName(cl.getName());
                        } catch (ClassNotFoundException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            KryoSmartStreamSerialImpl<?> kryoSerializer = new KryoSmartStreamSerialImpl<>(classes);
            for (Class<?> cl : classes) {
                serialCfg.addSerializerConfig(new SerializerConfig().setTypeClass(cl).setImplementation(kryoSerializer));
            }
        } catch (Exception e) {
            logger.error("Error", e);
        }
    }

    private static <T> void processKeyMap(HazelcastInstance hz, T key) {
        IMap<T, Integer> mapBin = hz.getMap("mapBin");
        IMap<T, Integer> mapObj = hz.getMap("mapObj");

        logger.debug("original key: {}", key);

        mapBin.set(key, 2);
        mapObj.set(key, 2);
        logger.debug("mapBin contains key: {}", mapBin.containsKey(key));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(key));

        @SuppressWarnings("unchecked")
        T[] mapBinKeys = (T[]) mapBin.keySet().toArray();
        @SuppressWarnings("unchecked")
        T[] mapObjKeys = (T[]) mapObj.keySet().toArray();
        logger.debug("mapBin keys: {}", Arrays.toString(mapBinKeys));
        logger.debug("mapObj keys: {}", Arrays.toString(mapObjKeys));

        logger.debug("mapBin contains key: {}", mapBin.containsKey(mapBinKeys[0]));
        logger.debug("mapObj contains key: {}", mapObj.containsKey(mapObjKeys[0]));

        mapBin.destroy();
        mapObj.destroy();
    }

    public static class KryoPersonStreamSerial extends Person {
        public KryoPersonStreamSerial() {
            /* need to be */
        }

        public KryoPersonStreamSerial(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }
    }

    public static class MapStreamSerial<K, V> extends HashMap<K, V> {
        /* serialization pattern only for this HashMap */
    }

    public static class PersonStreamSerial3 extends Person {
        private Map<?, ?> map;

        public PersonStreamSerial3() {
            /* need to be */
        }

        public PersonStreamSerial3(Map<?, ?> map, String name, String surname, String hobby) {
            super(name, surname, hobby);
            this.map = map;
        }

        public Map<?, ?> getMap() {
            return map;
        }

        public void setMap(Map<?, ?> map) {
            this.map = map;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("map", this.map)
                    .append("name", this.name)
                    .append("surname", this.surname)
                    .append("hobby", this.hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonStreamSerial3)) return false;
            if (!super.equals(o)) return false;
            PersonStreamSerial3 that = (PersonStreamSerial3) o;
            return Objects.equals(map, that.map) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), map);
        }
    }

    public static class PersonStreamSerial2 extends Person {
        private PersonStreamSerial streamSerial;

        public PersonStreamSerial2() {
            /* need to be */
        }

        public PersonStreamSerial2(PersonStreamSerial streamSerial) {
            super(streamSerial.getName(), streamSerial.getSurname(), streamSerial.getHobby());
            this.streamSerial = streamSerial;
        }

        public PersonStreamSerial getStreamSerial() {
            return streamSerial;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("streamSerial", streamSerial)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonStreamSerial2)) return false;
            PersonStreamSerial2 that = (PersonStreamSerial2) o;
            return Objects.equals(streamSerial, that.streamSerial) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), streamSerial);
        }
    }


    public static class PersonStreamSerial extends Person {
        public PersonStreamSerial() {
            /* need to be */
        }

        public PersonStreamSerial(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }
    }

    public static class PersonPortable3 extends Person implements Portable {
        private PersonPortable2 portable;

        public PersonPortable3() {
            /* need to be */
        }

        public PersonPortable3(PersonPortable2 portable) {
            super(portable.getName(), portable.getSurname(), portable.getHobby());
            this.portable = portable;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            if (portable == null) writer.writeNullPortable("portable", FACTORY_ID_200, CLASS_ID_202);
            else writer.writePortable("portable", portable);
            logger.debug("-->  write field portable: {}", portable);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.portable = reader.readPortable("portable");
            logger.debug("<--  read field portable: {}", portable);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_200;
        }

        @Override
        public int getClassId() {
            return CLASS_ID_203;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("portable", portable)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonPortable3)) return false;
            PersonPortable3 that = (PersonPortable3) o;
            return Objects.equals(portable, that.portable) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), portable);
        }
    }

    public static class PersonPortable2 extends Person implements Portable {
        private String secondName;

        public PersonPortable2() {
            /* need to be */
        }

        public PersonPortable2(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("secondName", secondName);
            writer.writeUTF("name", name);
            logger.debug("-->  write fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.name = reader.readUTF("name");
            this.secondName = reader.readUTF("secondName");
            logger.debug("<--  read fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_200;
        }

        @Override
        public int getClassId() {
            return CLASS_ID_202;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("secondName", secondName)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonPortable2)) return false;
            PersonPortable2 that = (PersonPortable2) o;
            return Objects.equals(secondName, that.secondName) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), secondName);
        }
    }

    public static class PersonPortable1 extends Person implements Portable {
        private String secondName;

        public PersonPortable1() {
            /* need to be */
        }

        public PersonPortable1(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
            writer.writeUTF("secondName", secondName);
            logger.debug("-->  write fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.secondName = reader.readUTF("secondName");
            this.name = reader.readUTF("name");
            logger.debug("<--  read fields name: {}, secondName: {}", name, secondName);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_200;
        }

        @Override
        public int getClassId() {
            return CLASS_ID_201;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("secondName", secondName)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonPortable1)) return false;
            PersonPortable1 that = (PersonPortable1) o;
            return Objects.equals(secondName, that.secondName) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), secondName);
        }
    }

    public static class PersonIdentDataSerial1 extends Person implements IdentifiedDataSerializable {

        public PersonIdentDataSerial1() {
            /* need to be */
        }

        public PersonIdentDataSerial1(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_100;
        }

        @Override
        public int getId() {
            return CLASS_ID_101;
        }
    }

    public static class PersonIdentDataSerial2 extends Person implements IdentifiedDataSerializable {

        public PersonIdentDataSerial2() {
            /* need to be */
        }

        public PersonIdentDataSerial2(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }

        @Override
        public int getFactoryId() {
            return FACTORY_ID_100;
        }

        @Override
        public int getId() {
            return CLASS_ID_102;
        }
    }


    public static class PersonDataSerial extends Person implements DataSerializable {

        public PersonDataSerial() {
            /* need to be */
        }

        public PersonDataSerial(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }
    }


    public static class PersonExternal extends Person implements Externalizable {

        public PersonExternal() {
            /* need to be */
        }

        public PersonExternal(String name, String surname, String hobby) {
            super(name, surname, hobby);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(name);
            logger.debug("-->  write field name: {}", name);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.name = in.readUTF();
            logger.debug("<--  read field name: {}", name);
        }
    }

    public static class PersonSerial extends Person implements Serializable {
        private static final long serialVersionUID = 1L;

        private String secondName; // only this field will be deserialized

        public PersonSerial(String name, String secondName, String surname, String hobby) {
            super(name, surname, hobby);
            this.secondName = secondName;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("secondName", secondName)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof PersonSerial)) return false;
            PersonSerial that = (PersonSerial) o;
            return Objects.equals(secondName, that.secondName) &&
                    super.equals(o);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), secondName);
        }
    }


    public abstract static class Person {
        protected /* not final */ String name;
        protected /* not final */ transient String surname;
        protected /* not final */ transient String hobby = "Rugby";
        protected static /* not final */ String password = "password123";

        Person() {
            /* need to be */
        }

        protected Person(String name, String surname, String hobby) {
            this.name = name;
            this.surname = surname;
            this.hobby = hobby;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Person)) return false;
            Person that = (Person) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(surname, that.surname) &&
                    Objects.equals(hobby, that.hobby);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, surname, hobby);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, SHORT_PREFIX_STYLE)
                    .append("name", this.name)
                    .append("surname", surname)
                    .append("hobby", hobby)
                    .append("password", password)
                    .toString();
        }

        public String getName() {
            return name;
        }

        public String getSurname() {
            return surname;
        }

        public String getHobby() {
            return hobby;
        }

        public static String getPassword() {
            return password;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setSurname(String surname) {
            this.surname = surname;
        }

        public void setHobby(String hobby) {
            this.hobby = hobby;
        }

        public static void setPassword(String password) {
            Person.password = password;
        }
    }
}
