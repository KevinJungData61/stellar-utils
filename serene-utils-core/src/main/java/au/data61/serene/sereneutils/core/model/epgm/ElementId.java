package au.data61.serene.sereneutils.core.model.epgm;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class ElementId implements Serializable {

    private final UUID uuid;

    public static final ElementId NULL_VALUE =
            new ElementId(new UUID(0,0));

    public ElementId(UUID uuid) {
        this.uuid = uuid;
    }

    public ElementId(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = (bytes.length == 16) ? bb.getLong() :
                (bytes.length == 12) ? bb.getInt() : 0;
        long low = bb.getLong();
        this.uuid = new UUID(high, low);
    }

    public static ElementId fromString(String id) {
        if (id.length() == 36) {
            return new ElementId(UUID.fromString(id));
        }
        byte[] bytes = new BigInteger(id, 16).toByteArray();
        return new ElementId(bytes);
    }

    @Override
    public String toString() {
        return this.uuid.toString();
    }

    @Override
    public int hashCode() {
        return this.uuid.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return this.uuid.equals(obj);
    }

}
