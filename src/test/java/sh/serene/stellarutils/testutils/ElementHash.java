package sh.serene.stellarutils.testutils;

import org.apache.spark.api.java.function.MapFunction;
import sh.serene.stellarutils.entities.EdgeCollection;
import sh.serene.stellarutils.entities.VertexCollection;
import sh.serene.stellarutils.entities.Element;

/**
 * Helper class to hash an element to an integer
 * @param <T>   vertex, edge, or graph head
 */
public class ElementHash<T extends Element> implements MapFunction<T,Integer> {

    @Override
    public Integer call(T element) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(element.getId().toString());
        stringBuilder.append(element.getLabel());
        if (element instanceof VertexCollection) {
            ((VertexCollection)element).getGraphs().forEach(elementId -> stringBuilder.append(elementId.toString()));
        }
        if (element instanceof EdgeCollection) {
            EdgeCollection e = (EdgeCollection) element;
            e.getGraphs().forEach(elementId -> stringBuilder.append(elementId.toString()));
            stringBuilder.append(e.getSrc().toString());
            stringBuilder.append(e.getDst().toString());
        }
        element.getProperties().entrySet().forEach(entry -> {
            stringBuilder.append(entry.getKey());
            stringBuilder.append(entry.getValue().toString());
        });
        return stringBuilder.toString().hashCode();
    }
}