package sh.serene.sereneutils.model.common;

import javax.xml.bind.DatatypeConverter;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * Unique element identifier based on UUID
 */
public class ElementId implements Serializable {

    private final static int BYTE_ARRAY_LENGTH = 16;

    /**
     * Internal UUID
     */
    private byte[] bytes;

    /**
     * Regex to match valid UUID string
     */
    private static final String uuidStrRegex =
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";

    /**
     * Regex to match valid 12 byte hex
     */
    private static final String hexStrRegex = "^[0-9a-fA-F]+$";

    /**
     * Create Element ID from UUID
     *
     * @param uuid  UUID
     */
    private ElementId(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[BYTE_ARRAY_LENGTH]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        this.bytes = bb.array();
    }

    /**
     * Create Element ID from byte array
     *
     * @param bytes     byte array
     */
    private ElementId(byte[] bytes) {
        if (bytes.length == BYTE_ARRAY_LENGTH) {
            this.bytes = bytes;
        } else if (bytes.length < BYTE_ARRAY_LENGTH) {
            this.bytes = new byte[BYTE_ARRAY_LENGTH];
            int start = BYTE_ARRAY_LENGTH - bytes.length;
            System.arraycopy(bytes, 0, this.bytes, start, bytes.length);
        } else {
            throw new IllegalArgumentException("Invalid identifier bytes");
        }

    }

    /**
     * Create a new random Element ID
     *
     * @return  Element ID
     */
    public static ElementId create() {
        return new ElementId(UUID.randomUUID());
    }

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public ElementId() { }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
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
            byte[] bytes = DatatypeConverter.parseHexBinary(id);
            return new ElementId(bytes);
        } else {
            throw new IllegalArgumentException("Invalid identifier string");
        }
    }

    @Override
    public String toString() {
        return DatatypeConverter.printHexBinary(this.bytes);
    }

    @Override
    public int hashCode() {
        byte[] intBytes = Arrays.copyOfRange(this.bytes, 12, 16);
        return ByteBuffer.wrap(intBytes).getInt();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ElementId) {
            return Arrays.equals(((ElementId) obj).getBytes(), this.bytes);
        } else {
            return false;
        }
    }

}
