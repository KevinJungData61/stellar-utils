package sh.serene.sereneutils.model.epgm;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PropertyValue implements Serializable {

    /**
     * Raw bytes
     */
    private byte[] bytes;

    public PropertyValue() { }

    public PropertyValue(byte[] bytes) {
        this.bytes = bytes;
    }

    private PropertyValue(Object object) throws IOException {
        serialize(object);
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public static PropertyValue create(Object value) {
        try {
            return new PropertyValue(value);
        } catch (IOException e) {
            return null;
        }
    }

    public Object getValue() {
        try {
            return deserialize();
        } catch (Exception e) {
            return null;
        }
    }

    public <T> T getValue(Class<T> type) {
        try {
            return type.cast(deserialize());
        } catch (Exception e) {
            return null;
        }
    }

    private void serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        this.bytes = out.toByteArray();
    }
    private Object deserialize() throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(this.bytes);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    @Override
    public String toString() {
        return this.getValue().toString();
    }

}
