package sh.serene.sereneutils.model.epgm;

import java.io.Serializable;

public class PropertyValue implements Serializable {

    private byte[] bytes;

    public PropertyValue() { }

    private PropertyValue(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public static PropertyValue create(String s) {
        return new PropertyValue(s.getBytes());
    }

    public static PropertyValue create(Object o) {
        return null;
    }

    @Override
    public String toString() {
        return new String(this.bytes);
    }

}
