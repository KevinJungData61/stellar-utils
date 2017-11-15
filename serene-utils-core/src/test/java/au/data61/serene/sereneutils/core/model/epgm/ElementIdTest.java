package au.data61.serene.sereneutils.core.model.epgm;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for Element ID class
 */
public class ElementIdTest {

    private final String strId = "01234567-89ab-cdef-0123-456789ABCDEF";
    private final String strIdAlt = "0123456789aBcDeF01234567";
    private final String strIdAltOut = "00000000-0123-4567-89aB-cDeF01234567";
    private final String strIdShort = "123456781234234234";
    private final String strIdLong = "123412341234123412341234123412341234";
    private final String strIdInvalid = "01234567-89ab-cdef-0123--4567890abcdef";

    @Test
    public void testToAndFromString() throws Exception {
        ElementId id = ElementId.fromString(strId);
        assertEquals(strId.toLowerCase(), id.toString());
        ElementId idAlt = ElementId.fromString(strIdAlt);
        assertEquals(strIdAltOut.toLowerCase(), idAlt.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShortString() throws Exception {
        ElementId.fromString(strIdShort);
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