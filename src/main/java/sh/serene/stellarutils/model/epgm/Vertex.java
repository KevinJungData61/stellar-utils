package sh.serene.stellarutils.model.epgm;

import java.io.Serializable;
import java.util.Map;

/**
 * Vertex of a Property Graph
 */
public class Vertex implements Element, Serializable, Cloneable {

    private ElementId id;
    private Map<String,PropertyValue> properties;
    private String label;
    private ElementId version;

    /**
     * Default constructor not to be used explicitly
     */
    @Deprecated
    public Vertex() { }

    private Vertex(
            final ElementId id,
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        this.id = id;
        this.properties = properties;
        this.label = label;
        this.version = version;
    }

    private Vertex(
            final String id,
            final Map<String,PropertyValue> properties,
            final String label,
            final String version
    ) {
        this.id = ElementId.fromString(id);
        this.properties = properties;
        this.label = label;
        this.version = ElementId.fromString(version);
    }

    /**
     * Copy constructor
     *
     * @param vertex
     */
    public Vertex(Vertex vertex) {
        this(vertex.getId(), vertex.getProperties(), vertex.getLabel(), vertex.version());
    }

    public Vertex clone() {
        return new Vertex(this);
    }

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier
     * @param properties    vertex properties
     * @param label         vertex label
     * @param version       id of first graph this version of the vertex was contained in
     * @returns             new vertex
     */
    public static Vertex create(
            final ElementId id,
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        return new Vertex(id, properties, label, version);
    }

    /**
     * Creates a vertex based on the given parameters. A unique ID is generated.
     *
     * @param properties    vertex properties
     * @param label         vertex label
     * @param version       id of first graph this version of the vertex was contained in
     * @return              new vertex
     */
    public static Vertex create(
            final Map<String,PropertyValue> properties,
            final String label,
            final ElementId version
    ) {
        return new Vertex(ElementId.create(), properties, label, version);
    }

    /**
     * Creates a vertex based on the given parameters
     *
     * @param id            vertex identifier string
     * @param properties    vertex properties
     * @param label         vertex label
     * @param version       id of first graph this version of the vertex was contained in
     * @return              new vertex
     */
    public static Vertex createFromStringIds(
            final String id,
            final Map<String,PropertyValue> properties,
            final String label,
            final String version
    ) {
        return new Vertex(id, properties, label, version);
    }

    /**
     * Create a vertex from a vertex collection
     *
     * @param vertexCollection  vertex collection
     * @return                  new vertex
     */
    public static Vertex createFromCollection(VertexCollection vertexCollection) {
        return new Vertex(
                vertexCollection.getId(),
                vertexCollection.getProperties(),
                vertexCollection.getLabel(),
                vertexCollection.version()
        );
    }

    @Override
    public ElementId getId() {
        return this.id;
    }

    @Override
    public void setId(ElementId id) {
        this.id = id;
    }

    @Override
    public Map<String,PropertyValue> getProperties() {
        return this.properties;
    }

    @Override
    public void setProperties(Map<String,PropertyValue> properties) {
        this.properties = properties;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public PropertyValue getProperty(String key) {
        return this.properties.get(key);
    }

    @Override
    public Object getPropertyValue(String key) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value();
    }

    @Override
    public <T> T getPropertyValue(String key, Class<T> type) {
        PropertyValue pv = this.getProperty(key);
        return (pv == null) ? null : pv.value(type);
    }

    @Override
    public ElementId version() {
        return this.version;
    }

    public ElementId getVersion() {
        return version();
    }

    public void setVersion(ElementId version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Vertex) {
            Vertex other = (Vertex) obj;
            return ((this.id.equals(other.getId()))
                    && (this.properties.equals(other.getProperties()))
                    && (this.label.equals(other.getLabel())));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

}
