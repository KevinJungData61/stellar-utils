package sh.serene.sereneutils.model.epgm;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class VertexTest {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;
    private List<ElementId> graphs;

    @Before
    public void setUp() {
        id = ElementId.create();
        properties = new HashMap<>();
        properties.put("string property", PropertyValue.create("123"));
        properties.put("integer property", PropertyValue.create(123));
        properties.put("double property", PropertyValue.create(1.23));
        properties.put("boolean property", PropertyValue.create(true));
        label = "label";
        graphs = Arrays.asList(ElementId.create());
    }

    @Test
    public void TestVertex() throws Exception {
        Vertex vertex = Vertex.create(id, properties, label, graphs);
        Vertex vertexFromStrings = Vertex.createFromStringIds(id.toString(),
                properties,
                label,
                Arrays.asList(graphs.get(0).toString()));
        assertEquals(vertex.getId(), vertexFromStrings.getId());
        assertEquals(vertex.getLabel(), vertexFromStrings.getLabel());
        assertEquals(vertex.getGraphs().get(0), vertexFromStrings.getGraphs().get(0));
        assertEquals(vertex, vertexFromStrings);
    }

}