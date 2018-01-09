package sh.serene.stellarutils.functions;

import sh.serene.stellarutils.entities.Element;
import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

public class VertexMergeFunction implements BinaryOperator<Vertex> {

    private ElementId version;
    private PropertiesMergeFunction propMergeFunc;

    public VertexMergeFunction(ElementId version, PropertiesMergeFunction propMergeFunc) {
        this.version = version;
        this.propMergeFunc = propMergeFunc;
    }
    /**
     * Applies this function to the given arguments.
     *
     * @param v  the first function argument
     * @param v2 the second function argument
     * @return the function result
     */
    @Override
    public Vertex apply(Vertex v, Vertex v2) {
        Map<String,PropertyValue> properties = propMergeFunc.merge(v, v2);
        if (properties.equals(v.getProperties())) {
            return v;
        } else if (properties.equals(v2.getProperties())) {
            return v2;
        } else {
            return Vertex.create(v.getId(), properties, v.getLabel(), this.version);
        }
    }

}
