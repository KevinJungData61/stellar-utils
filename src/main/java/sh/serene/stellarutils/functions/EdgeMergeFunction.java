package sh.serene.stellarutils.functions;

import sh.serene.stellarutils.entities.ElementId;
import sh.serene.stellarutils.entities.PropertyValue;
import sh.serene.stellarutils.entities.Edge;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;

public class EdgeMergeFunction implements BinaryOperator<Edge> {

    private ElementId version;
    private PropertiesMergeFunction propsMergeFunc;

    public EdgeMergeFunction(ElementId version, PropertiesMergeFunction propsMergeFunc) {
        this.version = version;
        this.propsMergeFunc = propsMergeFunc;
    }

    /**
     * Applies this function to the given arguments.
     *
     * @param e  the first function argument
     * @param e2 the second function argument
     * @return the function result
     */
    @Override
    public Edge apply(Edge e, Edge e2) {
        Map<String,PropertyValue> properties = propsMergeFunc.merge(e, e2);
        if (properties.equals(e.getProperties())) {
            return e;
        } else if (properties.equals(e2.getProperties())) {
            return e2;
        } else {
            return Edge.create(e.getId(), e.getSrc(), e.getDst(), properties, e.getLabel(), this.version);
        }
    }

}
