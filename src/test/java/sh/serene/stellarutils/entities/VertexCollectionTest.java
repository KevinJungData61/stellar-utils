package sh.serene.stellarutils.entities;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.VertexCollection;

import java.util.*;

import static org.junit.Assert.*;

public class VertexCollectionTest {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;
    private List<ElementId> graphs;
    private final String STR_PROP = "string property";
    private final String INT_PROP = "integer property";
    private final String DBL_PROP = "double property";
    private final String BOOL_PROP = "boolean property";

    @Before
    public void setUp() {
        id = ElementId.create();
        properties = new HashMap<>();
        properties.put(STR_PROP, PropertyValue.create("123"));
        properties.put(INT_PROP, PropertyValue.create(123));
        properties.put(DBL_PROP, PropertyValue.create(1.23));
        properties.put(BOOL_PROP, PropertyValue.create(true));
        label = "label";
        graphs = Collections.singletonList(ElementId.create());
    }

    private <T> void assertPropEquals(VertexCollection vertex, String key, Class<T> type) {
        assertEquals(properties.get(key), vertex.getProperty(key));
        assertEquals(properties.get(key).value(), vertex.getPropertyValue(key));
        assertEquals(properties.get(key).value(type), vertex.getPropertyValue(key, type));
    }

    @Test
    public void testCreate() throws Exception {
        VertexCollection vertex = VertexCollection.create(id, properties, label, graphs);
        assertEquals(id, vertex.getId());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label, vertex.getLabel());
        assertEquals(graphs, vertex.getGraphs());
    }

    @Test
    public void testCreateNoId() throws Exception {
        VertexCollection vertexNoId = VertexCollection.create(properties, label, graphs);
        assertPropEquals(vertexNoId, STR_PROP, String.class);
        assertPropEquals(vertexNoId, INT_PROP, Integer.class);
        assertPropEquals(vertexNoId, DBL_PROP, Double.class);
        assertPropEquals(vertexNoId, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertexNoId.getProperties());
        assertEquals(label, vertexNoId.getLabel());
        assertEquals(graphs, vertexNoId.getGraphs());
    }

    @Test
    public void testCreateFromStringIds() throws Exception {
        VertexCollection vertexStrId = VertexCollection.createFromStringIds(
                id.toString(),
                properties,
                label,
                Collections.singletonList(graphs.get(0).toString())
        );
        assertEquals(id, vertexStrId.getId());
        assertPropEquals(vertexStrId, STR_PROP, String.class);
        assertPropEquals(vertexStrId, INT_PROP, Integer.class);
        assertPropEquals(vertexStrId, DBL_PROP, Double.class);
        assertPropEquals(vertexStrId, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertexStrId.getProperties());
        assertEquals(label, vertexStrId.getLabel());
        assertEquals(graphs, vertexStrId.getGraphs());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCreateWithSetters() throws Exception {
        VertexCollection vertex = new VertexCollection();
        vertex.setId(id);
        vertex.setProperties(properties);
        vertex.setLabel(label);
        vertex.setGraphs(graphs);
        assertEquals(id, vertex.getId());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label, vertex.getLabel());
        assertEquals(graphs, vertex.getGraphs());
    }

    @Test
    public void testVertexEquals() throws Exception {
        VertexCollection vertex = VertexCollection.create(id, properties, label, graphs);
        VertexCollection vertexWithoutId = VertexCollection.create(properties, label, graphs);
        VertexCollection vertexFromStrings = VertexCollection.createFromStringIds(id.toString(),
                properties,
                label,
                Collections.singletonList(graphs.get(0).toString()));
        VertexCollection vertexClone = new VertexCollection(vertex);
        assertEquals(vertex, vertexFromStrings);
        assertEquals(vertex, vertexClone);
        assertNotEquals(vertex, vertexWithoutId);
    }

}