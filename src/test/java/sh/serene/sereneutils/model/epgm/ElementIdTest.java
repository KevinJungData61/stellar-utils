package sh.serene.sereneutils.model.epgm;

import org.junit.Test;
import sh.serene.sereneutils.model.epgm.ElementId;

import static org.junit.Assert.*;

/**
 * Unit tests for Element ID class
 */
public class ElementIdTest {

    private final String strId = "0123456789abcdef0123456789ABCDEF";
    private final String strIdAlt = "0123456789aBcDeF01234567";
    private final String strIdAltOut = "000000000123456789aBcDeF01234567";
    private final String strIdEmpty = "";
    private final String strIdLong = "123412341234123412341234123412341234";
    private final String strIdInvalid = "01--cd01245678----90abcdef";

    @Test
    public void testToAndFromString() throws Exception {
        ElementId id = ElementId.fromString(strId);
        assertEquals(strId.toUpperCase(), id.toString());
        ElementId idAlt = ElementId.fromString(strIdAlt);
        assertEquals(strIdAltOut.toUpperCase(), idAlt.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyString() throws Exception {
        ElementId.fromString(strIdEmpty);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLongString() throws Exception {
        ElementId.fromString(strIdLong);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidString() throws Exception {
        ElementId.fromString(strIdInvalid);
    }

    @Test
    public void testHashCode() throws Exception {
        ElementId id1 = ElementId.fromString(strIdAlt);
        ElementId id2 = ElementId.fromString(strIdAltOut);
        assertEquals(id1.hashCode(), id2.hashCode());
    }

    @Test
    public void testEquals() throws Exception {
        ElementId id1 = ElementId.fromString(strIdAlt);
        ElementId id2 = ElementId.fromString(strIdAltOut);
        assertTrue(id1.equals(id2));
    }

}