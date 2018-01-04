package sh.serene.stellarutils.io.impl.local;

import org.junit.Test;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;

import java.util.List;

import static org.junit.Assert.*;

public class LocalReaderTest {
    @Test
    public void json() throws Exception {
        //TODO
        LocalReader reader = new LocalReader();
        LocalGraphCollection gc = reader.json("small-yelp-hin.epgm");
        List<Edge> edges = gc.get(0).getEdges().asList();
        for (int i = 0; i < 10; i++) {
            Edge e = edges.get(i);
            System.out.println(String.format("[%s]%s: %s -> %s; %s",
                    e.getLabel(),
                    e.getId(),
                    e.getSrc(),
                    e.getDst(),
                    e.getProperties()));
        }
    }

}