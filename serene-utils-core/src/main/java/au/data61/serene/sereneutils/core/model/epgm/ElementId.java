package au.data61.serene.sereneutils.core.model.epgm;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Unique element identifier based on UUID
 */
public class ElementId implements Serializable {

    /**
     * Internal UUID
     */
    private final UUID uuid;

    /**
     * Regex to match valid UUID string
     */
    private static final String uuidStrRegex =
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

    /**
     * Regex to match valid 12 byte hex
     */
    private static final String hexStrRegex = "^[0-9a-fA-F]{24}$";

    /**
     * Create Element ID from UUID
     *
     * @param uuid  UUID
     */
    private ElementId(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Create Element ID from byte array
     *
     * @param bytes     byte array
     */
    private ElementId(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long high = bb.getInt();
        long low = bb.getLong();
        this.uuid = new UUID(high, low);
    }

    /**
     * Create Element ID from a valid UUID string or 12 byte hex string
     *
     * @param id    identifier string
     * @return      Element ID
     */
    public static ElementId fromString(String id) {

        if (id.matches(uuidStrRegex)) {
            return new ElementId(UUID.fromString(id));
        } else if (id.matches(hexStrRegex)) {
            byte[] bytes = new BigInteger(id, 16).toByteArray();
            return new ElementId(bytes);
        } else {
            throw new IllegalArgumentException("Invalid identifier string");
        }
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
        if (obj instanceof ElementId) {
            return this.hashCode() == obj.hashCode();
        } else {
            return false;
        }
    }

}
