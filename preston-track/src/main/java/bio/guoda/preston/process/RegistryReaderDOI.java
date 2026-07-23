package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.http.client.utils.URLEncodedUtils;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toIRI;

public class RegistryReaderDOI extends ProcessorReadOnly {


    public static final String GBIF_DOI_PART = "10.15468/";
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderDOI.class);

    public RegistryReaderDOI(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)) {
            String source = getVersionSource(statement).toString();
            if (source.contains(GBIF_DOI_PART)) {
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
            } else {
                List<Quad> nodes = new ArrayList<>();
                StatementEmitter emitter = new StatementEmitter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                };
                RegistryReaderDataDryad.emitOnDataDryadDoi(emitter, source);
                if (!nodes.isEmpty()) {
                    ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
                }
            }

        }
    }


    public static IRI toDataDryadVersionsQuery(String iriString) throws MalformedDOIException {
        DOI doi = DOI.create(iriString);
        String encodedDOI = URLEncodedUtils.formatSegments(doi.toString());
        return toIRI("https://datadryad.org/api/v2/datasets/doi%3A" + encodedDOI + "/versions");
    }

    static void parseGBIFDownloadHtmlPage(Quad statement, InputStream is, StatementsEmitter emitter) throws IOException {
        String htmlPage = IOUtils.toString(is, StandardCharsets.UTF_8);
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
