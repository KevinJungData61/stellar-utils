package sh.serene.stellarutils.functions;

import sh.serene.stellarutils.entities.Element;
import sh.serene.stellarutils.entities.PropertyValue;

import java.util.Map;

@FunctionalInterface
public interface PropertiesMergeFunction {

    /**
     * Applies this function to two elements to create a merged set of properties
     *
     * @param e     element 1
     * @param e2    element 2
     * @return      merged properties
     */
    Map<String,PropertyValue> merge(Element e, Element e2);
}
