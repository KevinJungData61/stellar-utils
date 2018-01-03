package sh.serene.stellarutils.testutils;

import sh.serene.stellarutils.entities.*;

import java.util.*;

public class TestGraphUtil {

    private static TestGraphUtil instance;

    private final List<GraphHead> graphHeadList;
    private final Map<String,ElementId> graphIdMap;
    private Map<ElementId,Map<String,PropertyValue>> graphPropertiesMap;
    private Map<ElementId,String> graphLabelMap;
    private final Map<ElementId,Integer> vertexCounts;
    private final Map<ElementId,Integer> edgeCounts;

    private Map<String,ElementId> vertexIdMap;
    private Map<ElementId,Map<String,PropertyValue>> vertexPropertiesMap;
    private Map<ElementId,String> vertexLabelMap;
    private Map<ElementId,ElementId> vertexVersionMap;
    private Map<ElementId,List<ElementId>> vertexGraphsMap;

    private Map<String,ElementId> edgeIdMap;
    private Map<ElementId,ElementId> edgeSrcMap;
    private Map<ElementId,ElementId> edgeDstMap;
    private Map<ElementId,Map<String,PropertyValue>> edgePropertiesMap;
    private Map<ElementId,String> edgeLabelMap;
    private Map<ElementId,ElementId> edgeVersionMap;
    private Map<ElementId,List<ElementId>> edgeGraphsMap;

    public static final String G_EMPTY = "empty";
    public static final String G_BASE_LINE = "base-line";
    public static final String G_NEW_INFO = "new-info";
    public static final String G_PRE_ER = "pre-er";
    public static final String G_POST_ER = "post-er";

    public static final String V_FRODO = "frodo";
    public static final String V_SAM = "sam";
    public static final String V_KFC = "kfc";
    public static final String V_MORDOR = "mordor";
    public static final String V_SHIRE = "shire";
    public static final String V_SAM2 = "sam2";
    public static final String V_MCD = "mcd";

    public static final String E_FRODO_FRIEND_SAM = "fFs";
    public static final String E_SAM_FRIEND_FRODO = "sFf";
    public static final String E_FRODO_REVIEW_KFC = "fRkfc";
    public static final String E_SAM_REVIEW_KFC = "sRkfc";
    public static final String E_KFC_LOC_MORDOR = "kfcLm";
    public static final String E_FRODO_LOC_MORDOR = "fLm";
    public static final String E_SAM_LOC_SHIRE = "sLsh";
    public static final String E_FRODO_BORN_SHIRE = "fBsh";
    public static final String E_SAM_BORN_SHIRE = "sBsh";
    public static final String E_SAM2_REVIEW_MCD = "s2Rmcd";
    public static final String E_SAM2_DUP_SAM = "s2Ds";

    private TestGraphUtil() {
        String[] graphKeys = {
                G_EMPTY,
                G_BASE_LINE,
                G_NEW_INFO,
                G_PRE_ER,
                G_POST_ER
        };
        graphIdMap = createIdMap(graphKeys);
        vertexCounts = new HashMap<>();
        edgeCounts = new HashMap<>();
        for (ElementId gid : graphIdMap.values()) {
            vertexCounts.put(gid, 0);
            edgeCounts.put(gid, 0);
        }
        graphPropertiesMap = buildGraphPropertiesMap(graphIdMap);
        graphLabelMap = buildGraphLabelMap(graphIdMap);

        // graph heads
        this.graphHeadList = Arrays.asList(
                GraphHead.create(graphIdMap.get(G_EMPTY), null, G_EMPTY),
                GraphHead.create(graphIdMap.get(G_BASE_LINE),
                        PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514948851)).build(),
                        G_BASE_LINE),
                GraphHead.create(graphIdMap.get(G_NEW_INFO),
                        PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build(),
                        G_NEW_INFO),
                GraphHead.create(graphIdMap.get(G_PRE_ER),
                        PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build(),
                        G_PRE_ER),
                GraphHead.create(graphIdMap.get(G_POST_ER),
                        PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build(),
                        G_POST_ER));

        // vertices
        String[] vertexKeys = {
                V_FRODO,
                V_SAM,
                V_KFC,
                V_MORDOR,
                V_SHIRE,
                V_SAM2,
                V_MCD
        };
        vertexIdMap = createIdMap(vertexKeys);
        vertexPropertiesMap = buildVertexPropertiesMap(vertexIdMap);
        vertexLabelMap = buildVertexLabelMap(vertexIdMap);
        vertexVersionMap = buildVertexVersionMap(vertexIdMap, graphIdMap);
        vertexGraphsMap = buildVertexGraphsMap(vertexIdMap, graphIdMap);

        // edges
        String[] edgeKeys = {
                E_FRODO_FRIEND_SAM,
                E_SAM_FRIEND_FRODO,
                E_FRODO_REVIEW_KFC,
                E_SAM_REVIEW_KFC,
                E_KFC_LOC_MORDOR,
                E_FRODO_LOC_MORDOR,
                E_SAM_LOC_SHIRE,
                E_FRODO_BORN_SHIRE,
                E_SAM_BORN_SHIRE,
                E_SAM2_REVIEW_MCD,
                E_SAM2_DUP_SAM
        };
        edgeIdMap = createIdMap(edgeKeys);
        edgeSrcMap = buildEdgeSrcMap(edgeIdMap, vertexIdMap);
        edgeDstMap = buildEdgeDstMap(edgeIdMap, vertexIdMap);
        edgePropertiesMap = buildEdgePropertiesMap(edgeIdMap);
        edgeLabelMap = buildEdgeLabelMap(edgeIdMap);
        edgeVersionMap = buildEdgeVersionMap(edgeIdMap, graphIdMap);
        edgeGraphsMap = buildEdgeGraphsMap(edgeIdMap, graphIdMap);

    }


    public static TestGraphUtil getInstance() {
        if (instance == null) {
            instance = new TestGraphUtil();
        }
        return instance;
    }

    public List<GraphHead> getGraphHeadList() {
        return new ArrayList<>(graphHeadList);
    }

    public List<VertexCollection> getVertexCollectionList() {
        List<VertexCollection> vertexCollectionList = new ArrayList<>();
        for (Map.Entry<String,ElementId> entry : vertexIdMap.entrySet()) {
            ElementId id = entry.getValue();
            vertexCollectionList.add(VertexCollection.create(
                    id,
                    vertexPropertiesMap.get(id),
                    vertexLabelMap.get(id),
                    vertexGraphsMap.get(id)
            ));
        }
        return vertexCollectionList;
    }

    public List<EdgeCollection> getEdgeCollectionList() {
        List<EdgeCollection> edgeCollectionList = new ArrayList<>();
        for (Map.Entry<String,ElementId> entry : edgeIdMap.entrySet()) {
            ElementId id = entry.getValue();
            edgeCollectionList.add(EdgeCollection.create(
                    id,
                    edgeSrcMap.get(id),
                    edgeDstMap.get(id),
                    edgePropertiesMap.get(id),
                    edgeLabelMap.get(id),
                    edgeGraphsMap.get(id)
            ));
        }
        return edgeCollectionList;
    }

    public List<ElementId> getGraphIdList() {
        return new ArrayList<>(this.graphIdMap.values());
    }

    public Map<String,PropertyValue> getGraphProperties(ElementId graphId) {
        return this.graphPropertiesMap.get(graphId);
    }

    public String getGraphLabel(ElementId graphId) {
        return this.graphLabelMap.get(graphId);
    }

    public int getVertexCount(ElementId graphId) {
        return this.vertexCounts.get(graphId);
    }

    public int getEdgeCount(ElementId graphId) {
        return this.edgeCounts.get(graphId);
    }

    public Map<String,PropertyValue> getVertexProperties(ElementId id) {
        return new HashMap<>(vertexPropertiesMap.get(id));
    }

    public String getVertexLabel(ElementId id) {
        return vertexLabelMap.get(id);
    }

    public ElementId getVertexVersion(ElementId id) {
        return vertexVersionMap.get(id);
    }

    public ElementId getEdgeSrc(ElementId id) {
        return edgeSrcMap.get(id);
    }

    public ElementId getEdgeDst(ElementId id) {
        return edgeDstMap.get(id);
    }

    public Map<String,PropertyValue> getEdgeProperties(ElementId id) {
        return new HashMap<>(edgePropertiesMap.get(id));
    }

    public String getEdgeLabel(ElementId id) {
        return edgeLabelMap.get(id);
    }

    public ElementId getEdgeVersion(ElementId id) {
        return edgeVersionMap.get(id);
    }

    private static class PropertiesBuilder {
        private Map<String,PropertyValue> properties;

        private PropertiesBuilder() {
            this.properties = new HashMap<>();
        }

        private PropertiesBuilder(Map<String,PropertyValue> properties) {
            this.properties = properties;
        }

        public static PropertiesBuilder create() {
            return new PropertiesBuilder();
        }

        public PropertiesBuilder with(String key, PropertyValue value) {
            Map<String,PropertyValue> propsNew = new HashMap<>(this.properties);
            propsNew.put(key, value);
            return new PropertiesBuilder(propsNew);
        }

        public Map<String,PropertyValue> build() {
            return new HashMap<>(this.properties);
        }
    }

    private static Map<String,ElementId> createIdMap(String[] keys) {
        Map<String,ElementId> map = new HashMap<>();
        for (String key : keys) {
            map.put(key, ElementId.create());
        }
        return map;
    }

    private Map<ElementId,Map<String,PropertyValue>> buildGraphPropertiesMap(Map<String,ElementId> ids) {
        Map<ElementId,Map<String,PropertyValue>> map = new HashMap<>();
        map.put(
                ids.get(G_EMPTY),
                new HashMap<>()
        );
        map.put(
                ids.get(G_BASE_LINE),
                PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514948851)).build()
        );
        map.put(
                ids.get(G_NEW_INFO),
                PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build()
        );
        map.put(
                ids.get(G_PRE_ER),
                PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build()
        );
        map.put(
                ids.get(G_POST_ER),
                PropertiesBuilder.create().with("timestamp", PropertyValue.create(1514958851)).build()
        );
        return map;
    }

    private Map<ElementId,String> buildGraphLabelMap(Map<String,ElementId> ids) {
        Map<ElementId,String> map = new HashMap<>();
        map.put(
                ids.get(G_EMPTY),
                G_EMPTY
        );
        map.put(
                ids.get(G_BASE_LINE),
                G_BASE_LINE
        );
        map.put(
                ids.get(G_NEW_INFO),
                G_NEW_INFO
        );
        map.put(
                ids.get(G_PRE_ER),
                G_PRE_ER
        );
        map.put(
                ids.get(G_POST_ER),
                G_POST_ER
        );
        return map;
    }

    private Map<ElementId,Map<String,PropertyValue>> buildVertexPropertiesMap(Map<String,ElementId> ids) {
        Map<ElementId,Map<String,PropertyValue>> map = new HashMap<>();
        map.put(
                ids.get(V_FRODO),
                PropertiesBuilder
                        .create()
                        .with("name", PropertyValue.create("Frodo"))
                        .with("race", PropertyValue.create("Hobbit"))
                        .build()
        );
        map.put(
                ids.get(V_SAM),
                PropertiesBuilder.create()
                        .with("name", PropertyValue.create("Sam"))
                        .with("race", PropertyValue.create("Hobbit"))
                        .build()
        );
        map.put(
                ids.get(V_KFC),
                PropertiesBuilder.create()
                        .with("name", PropertyValue.create("KFC"))
                        .build()
        );
        map.put(
                ids.get(V_MORDOR),
                PropertiesBuilder.create()
                        .with("name", PropertyValue.create("Mordor"))
                        .build()
        );
        map.put(
                ids.get(V_SHIRE),
                new HashMap<>()
        );
        map.put(
                ids.get(V_SAM2),
                PropertiesBuilder.create()
                        .with("name", PropertyValue.create("Sam"))
                        .with("race", PropertyValue.create("Hobbit"))
                        .build()
        );
        map.put(
                ids.get(V_MCD),
                PropertiesBuilder.create()
                        .with("name", PropertyValue.create("McDonald's"))
                        .build()
        );
        return map;
    }

    private Map<ElementId,String> buildVertexLabelMap(Map<String,ElementId> ids) {
        Map<ElementId,String> map = new HashMap<>();
        map.put(
                ids.get(V_FRODO),
                "Person"
        );
        map.put(
                ids.get(V_SAM),
                "Person"
        );
        map.put(
                ids.get(V_KFC),
                "Restaurant"
        );
        map.put(
                ids.get(V_MORDOR),
                "Location"
        );
        map.put(
                ids.get(V_SHIRE),
                "Location"
        );
        map.put(
                ids.get(V_SAM2),
                "Person"
        );
        map.put(
                ids.get(V_MCD),
                "Restaurant"
        );
        return map;
    }

    private Map<ElementId,ElementId> buildVertexVersionMap(Map<String,ElementId> ids, Map<String,ElementId> graphIds) {
        Map<ElementId,ElementId> map = new HashMap<>();
        map.put(
                ids.get(V_FRODO),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(V_SAM),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(V_KFC),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(V_MORDOR),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(V_SHIRE),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(V_SAM2),
                graphIds.get(G_NEW_INFO)
        );
        map.put(
                ids.get(V_MCD),
                graphIds.get(G_NEW_INFO)
        );
        return map;
    }

    private void putVertexInGraphs(Map<ElementId,List<ElementId>> map, ElementId vid, List<ElementId> graphs,
                                   Map<ElementId,Integer> vertexCounts) {
        map.put(vid, graphs);
        for (ElementId gid : graphs) {
            if (vertexCounts.containsKey(gid)) {
                vertexCounts.put(gid, vertexCounts.get(gid) + 1);
            } else {
                vertexCounts.put(gid, 1);
            }
        }
    }

    private Map<ElementId,List<ElementId>> buildVertexGraphsMap(Map<String,ElementId> ids, Map<String,ElementId> graphIds) {
        List<ElementId> baseLineGraphIds = Arrays.asList(
                graphIds.get(G_BASE_LINE), graphIds.get(G_PRE_ER), graphIds.get(G_POST_ER));
        List<ElementId> newInfoGraphIds = Arrays.asList(
                graphIds.get(G_NEW_INFO), graphIds.get(G_PRE_ER), graphIds.get(G_POST_ER));
        Map<ElementId,List<ElementId>> map = new HashMap<>();
        putVertexInGraphs(map,
                ids.get(V_FRODO),
                baseLineGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_SAM),
                baseLineGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_KFC),
                baseLineGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_MORDOR),
                baseLineGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_SHIRE),
                baseLineGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_SAM2),
                newInfoGraphIds,
                vertexCounts
        );
        putVertexInGraphs(map,
                ids.get(V_MCD),
                newInfoGraphIds,
                vertexCounts
        );
        return map;
    }

    private Map<ElementId,ElementId> buildEdgeSrcMap(Map<String,ElementId> ids, Map<String,ElementId> vids) {
        Map<ElementId,ElementId> map = new HashMap<>();
        map.put(
                ids.get(E_FRODO_FRIEND_SAM),
                vids.get(V_FRODO)
        );
        map.put(
                ids.get(E_SAM_FRIEND_FRODO),
                vids.get(V_SAM)
        );
        map.put(
                ids.get(E_FRODO_REVIEW_KFC),
                vids.get(V_FRODO)
        );
        map.put(
                ids.get(E_SAM_REVIEW_KFC),
                vids.get(V_SAM)
        );
        map.put(
                ids.get(E_KFC_LOC_MORDOR),
                vids.get(V_KFC)
        );
        map.put(
                ids.get(E_FRODO_LOC_MORDOR),
                vids.get(V_FRODO)
        );
        map.put(
                ids.get(E_SAM_LOC_SHIRE),
                vids.get(V_SAM)
        );
        map.put(
                ids.get(E_FRODO_BORN_SHIRE),
                vids.get(V_FRODO)
        );
        map.put(
                ids.get(E_SAM_BORN_SHIRE),
                vids.get(V_SAM)
        );
        map.put(
                ids.get(E_SAM2_REVIEW_MCD),
                vids.get(V_SAM2)
        );
        map.put(
                ids.get(E_SAM2_DUP_SAM),
                vids.get(V_SAM2)
        );
        return map;
    }


    private Map<ElementId,ElementId> buildEdgeDstMap(Map<String,ElementId> ids, Map<String,ElementId> vids) {
        Map<ElementId,ElementId> map = new HashMap<>();
        map.put(
                ids.get(E_FRODO_FRIEND_SAM),
                vids.get(V_SAM)
        );
        map.put(
                ids.get(E_SAM_FRIEND_FRODO),
                vids.get(V_FRODO)
        );
        map.put(
                ids.get(E_FRODO_REVIEW_KFC),
                vids.get(V_KFC)
        );
        map.put(
                ids.get(E_SAM_REVIEW_KFC),
                vids.get(V_KFC)
        );
        map.put(
                ids.get(E_KFC_LOC_MORDOR),
                vids.get(V_MORDOR)
        );
        map.put(
                ids.get(E_FRODO_LOC_MORDOR),
                vids.get(V_MORDOR)
        );
        map.put(
                ids.get(E_SAM_LOC_SHIRE),
                vids.get(V_SHIRE)
        );
        map.put(
                ids.get(E_FRODO_BORN_SHIRE),
                vids.get(V_SHIRE)
        );
        map.put(
                ids.get(E_SAM_BORN_SHIRE),
                vids.get(V_SHIRE)
        );
        map.put(
                ids.get(E_SAM2_REVIEW_MCD),
                vids.get(V_MCD)
        );
        map.put(
                ids.get(E_SAM2_DUP_SAM),
                vids.get(V_SAM)
        );
        return map;
    }

    private Map<ElementId,Map<String,PropertyValue>> buildEdgePropertiesMap(Map<String,ElementId> ids) {
        Map<ElementId,Map<String,PropertyValue>> map = new HashMap<>();
        map.put(
                ids.get(E_FRODO_FRIEND_SAM),
                new HashMap<>()
        );
        map.put(
                ids.get(E_SAM_FRIEND_FRODO),
                new HashMap<>()
        );
        map.put(
                ids.get(E_FRODO_REVIEW_KFC),
                PropertiesBuilder.create()
                        .with("stars", PropertyValue.create(3))
                        .build()
        );
        map.put(
                ids.get(E_SAM_REVIEW_KFC),
                PropertiesBuilder.create()
                        .with("stars", PropertyValue.create(5))
                        .build()
        );
        map.put(
                ids.get(E_KFC_LOC_MORDOR),
                new HashMap<>()
        );
        map.put(
                ids.get(E_FRODO_LOC_MORDOR),
                new HashMap<>()
        );
        map.put(
                ids.get(E_SAM_LOC_SHIRE),
                new HashMap<>()
        );
        map.put(
                ids.get(E_FRODO_BORN_SHIRE),
                new HashMap<>()
        );
        map.put(
                ids.get(E_SAM_BORN_SHIRE),
                new HashMap<>()
        );
        map.put(
                ids.get(E_SAM2_REVIEW_MCD),
                PropertiesBuilder.create()
                        .with("stars", PropertyValue.create(4))
                        .build()
        );
        map.put(
                ids.get(E_SAM2_DUP_SAM),
                new HashMap<>()
        );
        return map;
    }


    private Map<ElementId,String> buildEdgeLabelMap(Map<String,ElementId> ids) {
        Map<ElementId,String> map = new HashMap<>();
        map.put(
                ids.get(E_FRODO_FRIEND_SAM),
                "friends-with"
        );
        map.put(
                ids.get(E_SAM_FRIEND_FRODO),
                "friends-with"
        );
        map.put(
                ids.get(E_FRODO_REVIEW_KFC),
                "reviewed"
        );
        map.put(
                ids.get(E_SAM_REVIEW_KFC),
                "reviewed"
        );
        map.put(
                ids.get(E_KFC_LOC_MORDOR),
                "located-in"
        );
        map.put(
                ids.get(E_FRODO_LOC_MORDOR),
                "located-in"
        );
        map.put(
                ids.get(E_SAM_LOC_SHIRE),
                "located-in"
        );
        map.put(
                ids.get(E_FRODO_BORN_SHIRE),
                "born-in"
        );
        map.put(
                ids.get(E_SAM_BORN_SHIRE),
                "born-in"
        );
        map.put(
                ids.get(E_SAM2_REVIEW_MCD),
                "reviewed"
        );
        map.put(
                ids.get(E_SAM2_DUP_SAM),
                "duplicate-of"
        );
        return map;
    }

    private Map<ElementId,ElementId> buildEdgeVersionMap(Map<String,ElementId> ids, Map<String,ElementId> graphIds) {
        Map<ElementId,ElementId> map = new HashMap<>();
        map.put(
                ids.get(E_FRODO_FRIEND_SAM),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_SAM_FRIEND_FRODO),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_FRODO_REVIEW_KFC),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_SAM_REVIEW_KFC),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_KFC_LOC_MORDOR),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_FRODO_LOC_MORDOR),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_SAM_LOC_SHIRE),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_FRODO_BORN_SHIRE),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_SAM_BORN_SHIRE),
                graphIds.get(G_BASE_LINE)
        );
        map.put(
                ids.get(E_SAM2_REVIEW_MCD),
                graphIds.get(G_NEW_INFO)
        );
        map.put(
                ids.get(E_SAM2_DUP_SAM),
                graphIds.get(G_POST_ER)
        );
        return map;
    }

    private void putEdgeInGraphs(Map<ElementId,List<ElementId>> map, ElementId eid, List<ElementId> graphs, Map<ElementId,Integer> edgeCounts) {
        map.put(eid, graphs);
        for (ElementId gid : graphs) {
            if (edgeCounts.containsKey(gid)) {
                edgeCounts.put(gid, edgeCounts.get(gid) + 1);
            } else {
                edgeCounts.put(gid, 1);
            }
        }
    }

    private Map<ElementId,List<ElementId>> buildEdgeGraphsMap(Map<String,ElementId> ids, Map<String,ElementId> graphIds) {
        List<ElementId> baseLineGraphIds = Arrays.asList(
                graphIds.get(G_BASE_LINE), graphIds.get(G_PRE_ER), graphIds.get(G_POST_ER));
        List<ElementId> newInfoGraphIds = Arrays.asList(
                graphIds.get(G_NEW_INFO), graphIds.get(G_PRE_ER), graphIds.get(G_POST_ER));
        List<ElementId> postErGraphIds = Collections.singletonList(graphIds.get(G_POST_ER));
        Map<ElementId,List<ElementId>> map = new HashMap<>();
        putEdgeInGraphs(map,
                ids.get(E_FRODO_FRIEND_SAM),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM_FRIEND_FRODO),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_FRODO_REVIEW_KFC),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM_REVIEW_KFC),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_KFC_LOC_MORDOR),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_FRODO_LOC_MORDOR),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM_LOC_SHIRE),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_FRODO_BORN_SHIRE),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM_BORN_SHIRE),
                baseLineGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM2_REVIEW_MCD),
                newInfoGraphIds,
        edgeCounts);
        putEdgeInGraphs(map,
                ids.get(E_SAM2_DUP_SAM),
                postErGraphIds,
        edgeCounts);
        return map;
    }

}
