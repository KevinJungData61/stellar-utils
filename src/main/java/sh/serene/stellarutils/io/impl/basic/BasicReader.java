package sh.serene.stellarutils.io.impl.basic;

import sh.serene.stellarutils.graph.api.StellarGraphCollection;
import sh.serene.stellarutils.io.api.StellarReader;

import java.io.IOException;

/**
 * Basic Graph Collection Reader
 *
 */
public class BasicReader implements StellarReader {

    private final String fileFormat;

    public BasicReader() {
        this.fileFormat = "json";
    }

    private BasicReader(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    /**
     * Set file format. Supported formats may vary depending on implementation
     *
     * @param fileFormat file format
     * @return reader object
     */
    @Override
    public StellarReader format(String fileFormat) {
        return new BasicReader(fileFormat);
    }

    /**
     * Read graph collection from path.
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public StellarGraphCollection getGraphCollection(String path) throws IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Read graph collection from path in json format. This takes precedence over any previous file format setting
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public StellarGraphCollection json(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Read graph colleciton from path in parquet format. This takes precedence over any previous file format setting
     *
     * @param path input path
     * @return graph collection
     */
    @Override
    public StellarGraphCollection parquet(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
