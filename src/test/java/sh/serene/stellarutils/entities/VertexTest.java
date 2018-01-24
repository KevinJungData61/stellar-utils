package sh.serene.stellarutils.entities;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.entities.VertexCollection;

import java.util.*;

import static org.junit.Assert.*;

public class VertexTest {

    private Map<String,PropertyValue> properties;
    private String label1 = "first";
    private String label2 = "second";
    private final String STR_PROP = "string property";
    private final String INT_PROP = "integer property";
    private final String DBL_PROP = "double property";
    private final String BOOL_PROP = "boolean property";

    private <T> void assertPropEquals(Vertex vertex, String key, Class<T> type) {
        assertEquals(properties.get(key), vertex.getProperty(key));
        assertEquals(properties.get(key).value(), vertex.getPropertyValue(key));
        assertEquals(properties.get(key).value(type), vertex.getPropertyValue(key, type));
    }

    @Before
    public void setUp() throws Exception {
        this.properties = new HashMap<>();
        this.properties.put(STR_PROP, PropertyValue.create("value1"));
        this.properties.put(INT_PROP, PropertyValue.create(2));
        this.properties.put(DBL_PROP, PropertyValue.create(3.0));
        this.properties.put(BOOL_PROP, PropertyValue.create(true));
    }

    @Test
    public void testClone() throws Exception {
        Vertex vertex1 = Vertex.create(properties, label1, ElementId.create());
        Vertex vertex2 = vertex1.clone();
        vertex2.setLabel(label2);
        assertEquals(vertex1.getId(), vertex2.getId());
        assertEquals(vertex1.getVersion(), vertex2.getVersion());
        assertEquals(vertex1.getProperties(), vertex2.getProperties());
        assertEquals(label1, vertex1.getLabel());
        assertEquals(label2, vertex2.getLabel());
    }

    @Test
    public void create() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Vertex vertex = Vertex.create(id, properties, label1, version);
        assertEquals(id, vertex.getId());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label1, vertex.getLabel());
        assertEquals(version, vertex.version());
    }

    @Test
    public void create1() throws Exception {
        ElementId version = ElementId.create();
        Vertex vertex = Vertex.create(properties, label1, version);
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label1, vertex.getLabel());
        assertEquals(version, vertex.version());
    }

    @Test
    public void createFromStringIds() throws Exception {
        String id = ElementId.create().toString();
        String version = ElementId.create().toString();
        Vertex vertex = Vertex.createFromStringIds(id, properties, label1, version);
        assertEquals(id, vertex.getId().toString());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label1, vertex.getLabel());
        assertEquals(version, vertex.getVersion().toString());
    }

    @Test
    public void createFromCollection() throws Exception {
        List<ElementId> graphs = Collections.singletonList(ElementId.create());
        VertexCollection vertexCollection = VertexCollection.create(properties, label1, graphs);
        Vertex vertex = Vertex.createFromCollection(vertexCollection);
        assertEquals(vertexCollection.getId(), vertex.getId());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(vertexCollection.getProperties(), vertex.getProperties());
        assertEquals(vertexCollection.getLabel(), vertex.getLabel());
        assertEquals(vertexCollection.version(), vertex.version());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCreateWithSetters() throws Exception {
        Vertex vertex = new Vertex();
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        vertex.setId(id);
        vertex.setProperties(properties);
        vertex.setLabel(label1);
        vertex.setVersion(version);
        assertEquals(id, vertex.getId());
        assertPropEquals(vertex, STR_PROP, String.class);
        assertPropEquals(vertex, INT_PROP, Integer.class);
        assertPropEquals(vertex, DBL_PROP, Double.class);
        assertPropEquals(vertex, BOOL_PROP, Boolean.class);
        assertEquals(properties, vertex.getProperties());
        assertEquals(label1, vertex.getLabel());
        assertEquals(version, vertex.getVersion());
    }

    @Test
    public void equals() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Vertex vertex = Vertex.create(id, properties, label1, version);
        Vertex vertexIdentical = Vertex.create(id, properties, label1, version);
        Vertex vertexDiffVersion = Vertex.create(id, properties, label1, ElementId.create());
        Vertex vertexDiffLabel = Vertex.create(id, properties, label2, ElementId.create());
        Vertex vertexDiffProps = Vertex.create(id, new HashMap<>(), label1, ElementId.create());
        Vertex vertexDiffId = Vertex.create(properties, label1, ElementId.create());
        assertEquals(vertex, vertexIdentical);
        assertEquals(vertex, vertexDiffVersion);
        assertFalse(vertex.equals(vertexDiffLabel));
        assertFalse(vertex.equals(vertexDiffProps));
        assertFalse(vertex.equals(vertexDiffId));
    }

    @Test
    public void testHashCode() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Vertex vertex1 = Vertex.create(id, properties, label1, version);
        Vertex vertex2 = Vertex.create(id, properties, label1, version);
        assertEquals(vertex1.hashCode(), vertex2.hashCode());
    }

}