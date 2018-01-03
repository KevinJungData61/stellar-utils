package sh.serene.stellarutils.io.impl.local.json;


import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.GraphHead;
import sh.serene.stellarutils.entities.VertexCollection;

import java.util.function.Function;

public class JSONToLocal {

    private static class JSONToVertexCollection implements Function<String,VertexCollection> {

        @Override
        public VertexCollection apply(String s) {
            //TODO
            return null;
        }
    }

    private static class JSONToEdgeCollection implements Function<String,EdgeCollection> {
        @Override
        public EdgeCollection apply(String s) {
            //TODO
            return null;
        }
    }

    private static class JSONToGraphHead implements Function<String,GraphHead> {
        @Override
        public GraphHead apply(String s) {
            //TODO
            return null;
        }
    }

    public static JSONToVertexCollection toVertexCollection() {
        return new JSONToVertexCollection();
    }

    public static JSONToEdgeCollection toEdgeCollection() {
        return new JSONToEdgeCollection();
    }

    public static JSONToGraphHead toGraphHead() {
        return new JSONToGraphHead();
    }

}
