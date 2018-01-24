package sh.serene.stellarutils.entities;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;

import java.util.*;

import static org.junit.Assert.*;

public class EdgeCollectionTest {

    private ElementId id;
    private ElementId src;
    private ElementId dst;
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
        src = ElementId.create();
        dst = ElementId.create();
        properties = new HashMap<>();
        properties.put(STR_PROP, PropertyValue.create("123"));
        properties.put(INT_PROP, PropertyValue.create(123));
        properties.put(DBL_PROP, PropertyValue.create(1.23));
        properties.put(BOOL_PROP, PropertyValue.create(true));
        label = "label";
        graphs = Collections.singletonList(ElementId.create());
    }

    private <T> void assertPropEquals(EdgeCollection edge, String key, Class<T> type) {
        assertEquals(properties.get(key), edge.getProperty(key));
        assertEquals(properties.get(key).value(), edge.getPropertyValue(key));
        assertEquals(properties.get(key).value(type), edge.getPropertyValue(key, type));
    }

    @Test
    public void testCreate() throws Exception {
        EdgeCollection edge = EdgeCollection.create(id, src, dst, properties, label, graphs);
        assertEquals(id, edge.getId());
        assertEquals(src, edge.getSrc());
        assertEquals(dst, edge.getDst());
        assertPropEquals(edge, STR_PROP, String.class);
        assertPropEquals(edge, INT_PROP, Integer.class);
        assertPropEquals(edge, DBL_PROP, Double.class);
        assertPropEquals(edge, BOOL_PROP, Boolean.class);
        assertEquals(properties, edge.getProperties());
        assertEquals(label, edge.getLabel());
        assertEquals(graphs, edge.getGraphs());
    }

    @Test
    public void testCreateNoId() throws Exception {
        EdgeCollection edgeNoId = EdgeCollection.create(src, dst, properties, label, graphs);
        assertEquals(src, edgeNoId.getSrc());
        assertEquals(dst, edgeNoId.getDst());
        assertPropEquals(edgeNoId, STR_PROP, String.class);
        assertPropEquals(edgeNoId, INT_PROP, Integer.class);
        assertPropEquals(edgeNoId, DBL_PROP, Double.class);
        assertPropEquals(edgeNoId, BOOL_PROP, Boolean.class);
        assertEquals(properties, edgeNoId.getProperties());
        assertEquals(label, edgeNoId.getLabel());
        assertEquals(graphs, edgeNoId.getGraphs());
    }

    @Test
    public void testCreateFromStringIds() throws Exception {
        EdgeCollection edgeStrId = EdgeCollection.createFromStringIds(
                id.toString(),
                src.toString(),
                dst.toString(),
                properties,
                label,
                Collections.singletonList(graphs.get(0).toString())
        );
        assertEquals(id, edgeStrId.getId());
        assertEquals(src, edgeStrId.getSrc());
        assertEquals(dst, edgeStrId.getDst());
        assertPropEquals(edgeStrId, STR_PROP, String.class);
        assertPropEquals(edgeStrId, INT_PROP, Integer.class);
        assertPropEquals(edgeStrId, DBL_PROP, Double.class);
        assertPropEquals(edgeStrId, BOOL_PROP, Boolean.class);
        assertEquals(properties, edgeStrId.getProperties());
        assertEquals(label, edgeStrId.getLabel());
        assertEquals(graphs, edgeStrId.getGraphs());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testCreateWithSetters() throws Exception {
        EdgeCollection edge = new EdgeCollection();
        edge.setId(id);
        edge.setSrc(src);
        edge.setDst(dst);
        edge.setProperties(properties);
        edge.setLabel(label);
        edge.setGraphs(graphs);
        assertEquals(id, edge.getId());
        assertEquals(src, edge.getSrc());
        assertEquals(dst, edge.getDst());
        assertPropEquals(edge, STR_PROP, String.class);
        assertPropEquals(edge, INT_PROP, Integer.class);
        assertPropEquals(edge, DBL_PROP, Double.class);
        assertPropEquals(edge, BOOL_PROP, Boolean.class);
        assertEquals(properties, edge.getProperties());
        assertEquals(label, edge.getLabel());
        assertEquals(graphs, edge.getGraphs());
    }

    @Test
    public void testEdgeEquals() throws Exception {
        EdgeCollection edge = EdgeCollection.create(id, src, dst, properties, label, graphs);
        EdgeCollection edgeWithoutId = EdgeCollection.create(src, dst, properties, label, graphs);
        EdgeCollection edgeFromStrings = EdgeCollection.createFromStringIds(id.toString(),
                src.toString(),
                dst.toString(),
                properties,
                label,
                Collections.singletonList(graphs.get(0).toString()));
        EdgeCollection edgeClone = new EdgeCollection(edge);
        assertEquals(edge, edgeFromStrings);
        assertEquals(edge, edgeClone);
        assertNotEquals(edge, edgeWithoutId);
    }

}