package sh.serene.sereneutils.model.epgm;

import org.junit.Test;

import static org.junit.Assert.*;

public class PropertyValueTest {

    @Test
    public void testConstructors() throws Exception {
        int value = 123;
        PropertyValue pv = PropertyValue.create(value);
        PropertyValue pv2 = new PropertyValue(pv.getBytes());
        PropertyValue pv3 = new PropertyValue();
        pv3.setBytes(pv.getBytes());
        assertEquals(pv, pv2);
        assertEquals(pv, pv3);
    }

    @Test
    public void testIntegerPropertyValue() throws Exception {
        int value = 123;
        PropertyValue pv = PropertyValue.create(value);
        assertEquals(value, (int)pv.value());
        assertEquals(value, (int)pv.value(Integer.class));
        assertEquals(Integer.toString(value), pv.toString());
    }

    @Test
    public void testLongPropertyValue() throws Exception {
        long value = 123123123123123123L;
        PropertyValue pv = PropertyValue.create(value);
        assertEquals(value, pv.value());
        assertEquals(value, (long)pv.value(Long.class));
        assertEquals(Long.toString(value), pv.toString());
    }

    @Test
    public void testStringPropertyValue() throws Exception {
        String value = "123123123";
        PropertyValue pv = PropertyValue.create(value);
        assertEquals(value, pv.value());
        assertEquals(value, pv.value(String.class));
        assertEquals(value, pv.toString());
    }

    @Test
    public void testBooleanPropertyValue() throws Exception {
        boolean value = true;
        PropertyValue pv = PropertyValue.create(value);
        assertEquals(value, pv.value());
        assertEquals(value, pv.value(Boolean.class));
        assertEquals(Boolean.toString(value), pv.toString());
    }

    @Test
    public void testDoublePropertyValue() throws Exception {
        double value = 1.23;
        PropertyValue pv = PropertyValue.create(value);
        assertEquals(value, pv.value());
        assertEquals(value, pv.value(Double.class), 1e-7);
        assertEquals(Double.toString(value), pv.toString());
    }

}