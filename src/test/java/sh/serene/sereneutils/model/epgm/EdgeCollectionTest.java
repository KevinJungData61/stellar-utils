package sh.serene.sereneutils.model.epgm;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class EdgeCollectionTest {

    private ElementId id;
    private ElementId src;
    private ElementId dst;
    private Map<String,PropertyValue> properties;
    private String label;
    private List<ElementId> graphs;

    @Before
    public void setUp() {
        id = ElementId.create();
        src = ElementId.create();
        dst = ElementId.create();
        properties = new HashMap<>();
        properties.put("string property", PropertyValue.create("123"));
        properties.put("integer property", PropertyValue.create(123));
        properties.put("double property", PropertyValue.create(1.23));
        properties.put("boolean property", PropertyValue.create(true));
        label = "label";
        graphs = Arrays.asList(ElementId.create());
    }

    @Test
    public void TestEdge() throws Exception {
        EdgeCollection edge = EdgeCollection.create(id, src, dst, properties, label, graphs);
        EdgeCollection edgeFromStrings = EdgeCollection.createFromStringIds(id.toString(),
                src.toString(),
                dst.toString(),
                properties,
                label,
                Arrays.asList(graphs.get(0).toString()));
        assertEquals(edge.getId(), edgeFromStrings.getId());
        assertEquals(edge.getSrc(), edgeFromStrings.getSrc());
        assertEquals(edge.getDst(), edgeFromStrings.getDst());
        assertEquals(edge.getLabel(), edgeFromStrings.getLabel());
        assertEquals(edge.getGraphs().get(0), edgeFromStrings.getGraphs().get(0));
        assertEquals(edge, edgeFromStrings);
    }

}