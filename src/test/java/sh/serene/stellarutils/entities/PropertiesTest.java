package sh.serene.stellarutils.entities;

import org.junit.Test;
import sh.serene.stellarutils.entities.Properties;
import sh.serene.stellarutils.entities.PropertyValue;

import java.util.HashMap;

import static org.junit.Assert.*;

public class PropertiesTest {
    @Test
    public void create() throws Exception {
        Properties properties = Properties.create();
        assertEquals(new HashMap<String,PropertyValue>(), properties.getMap());
    }

    @Test
    public void addBoolean() throws Exception {
        Properties properties = Properties.create();
        properties.add("true", true);
        properties.add("false", false);
        assertTrue(properties.getMap().get("true").value(Boolean.class));
        assertFalse(properties.getMap().get("false").value(Boolean.class));
    }

    @Test
    public void addInteger() throws Exception {
        int zero = 0;
        int big = 893082491;
        int neg = -823794231;
        Properties properties = Properties.create();
        properties.add("zero", zero);
        properties.add("big", big);
        properties.add("neg", neg);
        assertEquals(zero, (int)properties.getMap().get("zero").value(Integer.class));
        assertEquals(big, (int)properties.getMap().get("big").value(Integer.class));
        assertEquals(neg, (int)properties.getMap().get("neg").value(Integer.class));
    }

    @Test
    public void addLong() throws Exception {
        long zero = 0;
        long big = 102738947392084L;
        long neg = -1208391580283420L;
        Properties properties = Properties.create();
        properties.add("zero", zero);
        properties.add("big", big);
        properties.add("neg", neg);
        assertEquals(zero, (long)properties.getMap().get("zero").value(Long.class));
        assertEquals(big, (long)properties.getMap().get("big").value(Long.class));
        assertEquals(neg, (long)properties.getMap().get("neg").value(Long.class));
    }

    @Test
    public void addDouble() throws Exception {
        double zero = 0.;
        double pos = 23453.348223;
        double neg = -1.2384739;
        double delta = 1e-7;
        Properties properties = Properties.create();
        properties.add("zero", zero);
        properties.add("pos", pos);
        properties.add("neg", neg);
        assertEquals(zero, properties.getMap().get("zero").value(Double.class), delta);
        assertEquals(pos, properties.getMap().get("pos").value(Double.class), delta);
        assertEquals(neg, properties.getMap().get("neg").value(Double.class), delta);
    }

    @Test
    public void addString() throws Exception {
        String empty = "";
        String small = "string";
        String large = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut " +
                "labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris " +
                "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit " +
                "esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt " +
                "in culpa qui officia deserunt mollit anim id est laborum";
        Properties properties = Properties.create();
        properties.add("empty", empty);
        properties.add("small", small);
        properties.add("large", large);
        assertEquals(empty, properties.getMap().get("empty").value(String.class));
        assertEquals(small, properties.getMap().get("small").value(String.class));
        assertEquals(large, properties.getMap().get("large").value(String.class));
    }

}