package sh.serene.stellarutils.graph.impl.local;

import org.junit.Before;
import org.junit.Test;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.api.StellarGraphBuffer;
import sh.serene.stellarutils.io.api.StellarReader;
import sh.serene.stellarutils.io.impl.local.LocalReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class LocalBackEndFactoryTest {

    private LocalBackEndFactory bef;

    @Before
    public void setUp() throws Exception {
        bef = new LocalBackEndFactory();
    }

    @Test
    public void testCreateMemoryEmpty() throws Exception {
        LocalGraphMemory<Vertex> mem = bef.createMemory(new ArrayList<>(), Vertex.class);
        assertEquals(new ArrayList<>(), mem.asList());
        assertEquals(0L, mem.size());
    }

    @Test
    public void testCreateMemory() throws Exception {
        ElementId version = ElementId.create();
        List<Vertex> vertices = Arrays.asList(
                Vertex.create(null, "vertex", version),
                Vertex.create(null, "vertex", version)
                );
        LocalGraphMemory<Vertex> mem = bef.createMemory(vertices, Vertex.class);
        assertEquals(vertices, mem.asList());
        assertEquals(vertices.size(), mem.size());
    }

    @Test
    public void reader() throws Exception {
        StellarReader reader = bef.reader();
        assertTrue(reader != null);
    }

    @Test
    public void createGraph() throws Exception {
        StellarGraphBuffer buf = bef.createGraph("graph", null);
        assertTrue(buf != null);
    }

}