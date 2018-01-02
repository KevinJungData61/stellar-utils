package sh.serene.stellarutils.io.impl.local;

import sh.serene.stellarutils.graph.impl.local.LocalGraphCollection;
import sh.serene.stellarutils.io.api.StellarWriter;

import java.io.IOException;

public class LocalWriter implements StellarWriter {

    private final LocalGraphCollection graphCollection;
    private final String fileFormat;

    public LocalWriter(LocalGraphCollection graphCollection) {
        this.graphCollection = graphCollection;
        this.fileFormat = "json";
    }

    private LocalWriter(LocalGraphCollection graphCollection, String fileFormat) {
        this.graphCollection = graphCollection;
        this.fileFormat = fileFormat;
    }

    /**
     * Set file format. Supported file formats may vary depending on implementation.
     *
     * @param fileFormat file format
     * @return writer object
     */
    @Override
    public StellarWriter format(String fileFormat) {
        return new LocalWriter(this.graphCollection, fileFormat);
    }

    /**
     * Save graph collection to path.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean save(String path) throws IOException {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Save graph collection to path in json format. This takes precedence over any previous file format setting.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean json(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Save graph collection to path in parquet format. This takes precedence over any previous file format setting.
     *
     * @param path output path
     * @return success
     */
    @Override
    public boolean parquet(String path) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
