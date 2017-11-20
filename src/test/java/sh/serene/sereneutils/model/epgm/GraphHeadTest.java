package sh.serene.sereneutils.model.epgm;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GraphHeadTest {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;

    @Before
    public void setUp() {
        id = ElementId.create();
        properties = new HashMap<>();
        properties.put("string property", PropertyValue.create("123"));
        properties.put("integer property", PropertyValue.create(123));
        properties.put("double property", PropertyValue.create(1.23));
        properties.put("boolean property", PropertyValue.create(true));
        label = "label";
    }

    @Test
    public void TestGraphHead() throws Exception {
        GraphHead graphHead = GraphHead.create(id, properties, label);
        GraphHead graphHeadFromStrings = GraphHead.createFromStringIds(id.toString(),
                properties,
                label);
        assertEquals(graphHead.getId(), graphHeadFromStrings.getId());
        assertEquals(graphHead.getLabel(), graphHeadFromStrings.getLabel());
        assertEquals(graphHead, graphHeadFromStrings);
    }

}