package sh.serene.sereneutils.model.epgm;

import java.io.*;

/**
 * Container for property value
 *
 */
public class PropertyValue implements Serializable {

    /**
     * Raw bytes
     */
    private byte[] bytes;

    /**
     * Default constructor
     */
    public PropertyValue() { }

    /**
     * Create PropertyValue from raw bytes
     * @param bytes
     */
    public PropertyValue(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * Create PropertyValue from an object
     *
     * @param object
     * @throws IOException
     */
    private PropertyValue(Object object) throws IOException {
        serialize(object);
    }


    /**
     * Create PropertyValue from an object. Returns null if object could not be serialized into a byte array
     *
     * @param value         Object to transform into PropertyValue
     * @return              PropertyValue
     */
    public static PropertyValue create(Object value) {
        try {
            return new PropertyValue(value);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Value stored in PropertyValue
     *
     * @return      stored object
     */
    public Object value() {
        try {
            return deserialize();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Value stored in PropertyValue
     *
     * @param type      type for casting the returned object
     * @param <T>
     * @return          stored object
     */
    public <T> T value(Class<T> type) {
        try {
            return type.cast(deserialize());
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Serialize the object to byte array
     *
     * @param obj
     * @throws IOException
     */
    private void serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        this.bytes = out.toByteArray();
    }

    /**
     * Deserialize object from byte array
     *
     * @return stored object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private Object deserialize() throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(this.bytes);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }


    @Override
    public String toString() {
        return this.value().toString();
    }

}
