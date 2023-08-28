package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;

public class ArchivingLogger extends StatementsListenerAdapter {
    private PersistingLocal persistingLocal;
    private final BlobStore logStore;
    private final HexaStore hexastore;
    private final ActivityContext ctx;
    File tmpArchive;
    OutputStream os;
    StatementsListener listener;

    public ArchivingLogger(
            PersistingLocal persistingLocal,
            BlobStore provStore,
            HexaStore provIndex,
            ActivityContext ctx) {
        this.persistingLocal = persistingLocal;
        this.logStore = provStore;
        this.hexastore = provIndex;
        this.ctx = ctx;
        tmpArchive = null;
        os = null;
        listener = null;
    }

    @Override
    public void on(Quad statement) {
        if (listener != null) {
            listener.on(statement);
        }
    }

    void start() throws IOException {
        tmpArchive = File.createTempFile("archive", "nq", persistingLocal.getTmpDir());
        os = IOUtils.buffer(new FileOutputStream(tmpArchive));
        listener = new StatementLoggerNQuads(os);
    }

    void stop() throws IOException {
        if (tmpArchive != null && tmpArchive.exists() && os != null && listener != null) {
            os.flush();
            os.close();

            os = null;

            try (FileInputStream is = new FileInputStream(tmpArchive)) {
                IRI newVersion = logStore.put(is);

                if (persistingLocal.isAnchored()) {
                    hexastore.put(Pair.of(HAS_PREVIOUS_VERSION, persistingLocal.getProvenanceAnchor()), newVersion);
                } else {
                    IRI previousVersion = VersionUtil.findMostRecentVersion(persistingLocal.getProvenanceAnchor(), hexastore);
                    if (previousVersion == null) {
                        hexastore.put(RefNodeConstants.PROVENANCE_ROOT_QUERY, newVersion);
                    } else {
                        hexastore.put(Pair.of(HAS_PREVIOUS_VERSION, previousVersion), newVersion);
                    }
                }
            }
        }

    }

    void destroy() {
        if (os != null) {
            try {
                os.flush();
                os.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (tmpArchive != null) {
            FileUtils.deleteQuietly(tmpArchive);
        }
    }
}
