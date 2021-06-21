package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.cxf.helpers.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderDOI extends ProcessorReadOnly {


    public static final String GBIF_DOI_PART = "10.15468/";
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderDOI.class);

    public RegistryReaderDOI(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(GBIF_DOI_PART)) {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is;
            try {
                is = get(currentPage);
                if (is != null) {
                    parseGBIFDownloadHtmlPage(statement, is, this);
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }


        }
    }

    static void parseGBIFDownloadHtmlPage(Quad statement, InputStream is, StatementsEmitter emitter) throws IOException {
        String htmlPage = IOUtils.toString(is, StandardCharsets.UTF_8.name());
        String[] split = htmlPage.split("key:\\s+");
        if (split.length > 1) {
            String[] split1 = split[1].split("'");
            if (split1.length > 2) {
                String gbifDownloadKey = split1[1];
                List<Quad> nodes = new ArrayList<>();
                nodes.add(RefNodeFactory.toStatement(toIRI("https://api.gbif.org/v1/occurrence/download/" + gbifDownloadKey), HAS_VERSION, RefNodeFactory.toBlank()));
                ActivityUtil.emitAsNewActivity(nodes.stream(), emitter, statement.getGraphName());

            }
        }
    }


}
