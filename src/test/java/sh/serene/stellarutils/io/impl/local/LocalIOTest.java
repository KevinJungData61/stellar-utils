package sh.serene.stellarutils.io.impl.local;

import org.json.simple.JSONObject;
import org.junit.Test;
import sh.serene.stellarutils.entities.Edge;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.Vertex;
import sh.serene.stellarutils.graph.impl.local.LocalGraph;
import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.testutils.TestGraphUtil;

import java.util.List;

import static org.junit.Assert.*;

public class LocalIOTest {
    @Test
    public void json() throws Exception {
        TestGraphUtil util = TestGraphUtil.getInstance();
        LocalGraphCollection graphCollectionWrite = new LocalGraphCollection(
                util.getGraphHeadList(), util.getVertexCollectionList(), util.getEdgeCollectionList()
        );
        graphCollectionWrite.write().json("./tmp.epgm");
        LocalGraphCollection graphCollectionRead = (new LocalReader()).json("./tmp.epgm");
        for (ElementId graphId : util.getGraphIdList()) {
            LocalGraph g = graphCollectionRead.get(graphId);
            for (Edge e : g.getEdges().asList()) {
                System.out.println(String.format(
                        "[%s] %s : %s -> %s; %s",
                        e.getLabel(), e.getId(), e.getSrc(), e.getDst(), e.getProperties()
                ));
            }
        }
    }

}