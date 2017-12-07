package sh.serene.stellarutils.model.epgm;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class VertexTest {

    private Map<String,PropertyValue> properties;
    private String label1 = "first";
    private String label2 = "second";

    @Before
    public void setUp() throws Exception {
        this.properties = new HashMap<>();
        this.properties.put("key1", PropertyValue.create("value1"));
        this.properties.put("key2", PropertyValue.create(2));
        this.properties.put("key3", PropertyValue.create(3.0));
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
        assertEquals(properties, vertex.getProperties());
        assertEquals(label1, vertex.getLabel());
        assertEquals(version, vertex.version());
    }

    @Test
    public void create1() throws Exception {
        ElementId version = ElementId.create();
        Vertex vertex = Vertex.create(properties, label1, version);
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
        assertEquals(vertexCollection.getProperties(), vertex.getProperties());
        assertEquals(vertexCollection.getLabel(), vertex.getLabel());
        assertEquals(vertexCollection.version(), vertex.version());
    }

    @Test
    public void equals() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Vertex vertex = Vertex.create(id, properties, label1, version);
        Vertex vertexIdentical = Vertex.create(id, properties, label1, version);
        Vertex vertexDiffVersion = Vertex.create(id, properties, label1, ElementId.create());
        Vertex vertexDiffLabel = Vertex.create(id, properties, label2, ElementId.create());
        Vertex vertexDiffProps = Vertex.create(new HashMap<>(), label1, ElementId.create());
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