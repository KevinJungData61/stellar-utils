package sh.serene.stellarutils.entities;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class EdgeTest {

    private ElementId src;
    private ElementId dst;
    private Map<String,PropertyValue> properties;
    private String label1 = "first";
    private String label2 = "second";

    @Before
    public void setUp() throws Exception {
        this.src = ElementId.create();
        this.dst = ElementId.create();
        this.properties = new HashMap<>();
        this.properties.put("key1", PropertyValue.create("value1"));
        this.properties.put("key2", PropertyValue.create(2));
        this.properties.put("key3", PropertyValue.create(3.0));
    }

    @Test
    public void testClone() throws Exception {
        Edge edge1 = Edge.create(src, dst, properties, label1, ElementId.create());
        Edge edge2 = edge1.clone();
        edge2.setLabel(label2);
        assertEquals(edge1.getId(), edge2.getId());
        assertEquals(edge1.getSrc(), edge2.getSrc());
        assertEquals(edge1.getDst(), edge2.getDst());
        assertEquals(edge1.getVersion(), edge2.getVersion());
        assertEquals(edge1.getProperties(), edge2.getProperties());
        assertEquals(label1, edge1.getLabel());
        assertEquals(label2, edge2.getLabel());
    }

    @Test
    public void create() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Edge edge = Edge.create(id, src, dst, properties, label1, version);
        assertEquals(id, edge.getId());
        assertEquals(src, edge.getSrc());
        assertEquals(dst, edge.getDst());
        assertEquals(properties, edge.getProperties());
        assertEquals(label1, edge.getLabel());
        assertEquals(version, edge.version());
    }

    @Test
    public void create1() throws Exception {
        ElementId version = ElementId.create();
        Edge edge = Edge.create(src, dst, properties, label1, version);
        assertEquals(src, edge.getSrc());
        assertEquals(dst, edge.getDst());
        assertEquals(properties, edge.getProperties());
        assertEquals(label1, edge.getLabel());
        assertEquals(version, edge.version());
    }

    @Test
    public void createFromStringIds() throws Exception {
        String id = ElementId.create().toString();
        String version = ElementId.create().toString();
        Edge edge = Edge.createFromStringIds(id, src.toString(), dst.toString(), properties, label1, version);
        assertEquals(id, edge.getId().toString());
        assertEquals(src, edge.getSrc());
        assertEquals(dst, edge.getDst());
        assertEquals(properties, edge.getProperties());
        assertEquals(label1, edge.getLabel());
        assertEquals(version, edge.getVersion().toString());
    }

    @Test
    public void createFromCollection() throws Exception {
        List<ElementId> graphs = Collections.singletonList(ElementId.create());
        EdgeCollection edgeCollection = EdgeCollection.create(src, dst, properties, label1, graphs);
        Edge edge = Edge.createFromCollection(edgeCollection);
        assertEquals(edgeCollection.getId(), edge.getId());
        assertEquals(edgeCollection.getSrc(), edge.getSrc());
        assertEquals(edgeCollection.getDst(), edge.getDst());
        assertEquals(edgeCollection.getProperties(), edge.getProperties());
        assertEquals(edgeCollection.getLabel(), edge.getLabel());
        assertEquals(edgeCollection.version(), edge.version());
    }

    @Test
    public void equals() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Edge edge = Edge.create(id, src, dst, properties, label1, version);
        Edge edgeIdentical = Edge.create(id, src, dst, properties, label1, version);
        Edge edgeDiffVersion = Edge.create(id, src, dst, properties, label1, ElementId.create());
        Edge edgeDiffLabel = Edge.create(id, src, dst, properties, label2, ElementId.create());
        Edge edgeDiffProps = Edge.create(id, src, dst, new HashMap<>(), label1, ElementId.create());
        Edge edgeDiffDst = Edge.create(id, src, ElementId.create(), properties, label1, ElementId.create());
        Edge edgeDiffSrc = Edge.create(id, ElementId.create(), dst, properties, label1, ElementId.create());
        Edge edgeDiffId = Edge.create(src, dst, properties, label1, ElementId.create());
        assertEquals(edge, edgeIdentical);
        assertEquals(edge, edgeDiffVersion);
        assertFalse(edge.equals(edgeDiffLabel));
        assertFalse(edge.equals(edgeDiffProps));
        assertFalse(edge.equals(edgeDiffDst));
        assertFalse(edge.equals(edgeDiffSrc));
        assertFalse(edge.equals(edgeDiffId));
    }

    @Test
    public void testHashCode() throws Exception {
        ElementId id = ElementId.create();
        ElementId version = ElementId.create();
        Edge edge1 = Edge.create(id, src, dst, properties, label1, version);
        Edge edge2 = Edge.create(id, src, dst, properties, label1, version);
        assertEquals(edge1.hashCode(), edge2.hashCode());
    }

}