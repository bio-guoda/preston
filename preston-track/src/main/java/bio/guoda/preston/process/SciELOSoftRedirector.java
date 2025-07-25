package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.tika.detect.TextDetector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.globalbioticinteractions.doi.DOI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.ALTERNATE_OF;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class SciELOSoftRedirector extends ProcessorReadOnly {
    public static final String SCI_ELO_URL_PART = "scielo.php?script=sci_pdf";
    public static final Pattern CHILE_PATTERN = Pattern.compile("http[s]{0,1}://www.scielo.cl/.*pid=(?<pid>[A-Za-z0-9-]+)");
    public static final Pattern BRAZIL_PATTERN = Pattern.compile("http[s]{0,1}://www.scielo.br/.*pid=(?<pid>[A-Za-z0-9-]+)");
    private static final Logger LOG = LoggerFactory.getLogger(SciELOSoftRedirector.class);

    private boolean inferDOIs = false;

    public SciELOSoftRedirector(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    static String inferChileDOI(String scieloUrl) {
        String scieloDOI = "";
        Matcher matcher = CHILE_PATTERN.matcher(scieloUrl);
        if (matcher.matches()) {
            scieloDOI = new DOI("4067", StringUtils.upperCase(matcher.group("pid"))).toURI().toString();
        }
        return scieloDOI;
    }

    static String inferBrazilDOI(String scieloUrl) {
        String scieloDOI = "";
        Matcher matcher = BRAZIL_PATTERN.matcher(scieloUrl);
        if (matcher.matches()) {
            scieloDOI = new DOI("1590", StringUtils.upperCase(matcher.group("pid"))).toURI().toString();
        }
        return scieloDOI;
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(SCI_ELO_URL_PART)) {
            ArrayList<Quad> nodes = new ArrayList<Quad>();
            try {
                IRI version = (IRI) getVersion(statement);
                InputStream in = get(version);
                if (in != null) {
                    StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {
                        @Override
                        public void emit(Quad statement) {
                            nodes.add(statement);
                        }
                    };
                    parse(emitter,
                            in,
                            getVersionSource(statement),
                            inferDOIs
                    );
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
            if (!nodes.isEmpty()) {
                ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
            }
        }
    }

    static void parse(StatementsEmitter emitter, InputStream is, IRI source, boolean inferDOIs1) throws IOException {
        if (inferDOIs1) {
            String doi = inferChileDOI(source.getIRIString());
            doi = StringUtils.isBlank(doi) ? inferBrazilDOI(source.getIRIString()) : doi;
            if (StringUtils.isNotBlank(doi)) {
                emitter.emit(RefNodeFactory.toStatement(source, ALTERNATE_OF, RefNodeFactory.toIRI(doi)));
            }
        }

        BufferedInputStream bis = IOUtils.buffer(is);

        Metadata metadata = new Metadata();
        MediaType detectedType = new TextDetector().detect(bis, metadata);

        if (MediaType.TEXT_PLAIN.equals(detectedType)) {
            LineIterator lineIterator = IOUtils.lineIterator(bis, StandardCharsets.UTF_8);
            while (lineIterator.hasNext()) {
                String firstChunk = lineIterator.nextLine();
                Pattern redirectUrlPattern = Pattern.compile(".*<script>.*setTimeout.*window.location=\\\"(?<pdfUrl>.*)\\\".*</script>.*", Pattern.MULTILINE);
                Matcher matcher = redirectUrlPattern.matcher(firstChunk);
                if (matcher.matches()) {
                    String pdfUrl = matcher.group("pdfUrl");
                    try {
                        emitter.emit(RefNodeFactory.toStatement(source, ALTERNATE_OF, RefNodeFactory.toIRI(pdfUrl)));
                        emitter.emit(RefNodeFactory.toStatement(RefNodeFactory.toIRI(pdfUrl), HAS_VERSION, RefNodeFactory.toBlank()));
                        break;
                    } catch (IllegalArgumentException ex) {
                        LOG.warn("failed to parse redirect candidate [" + pdfUrl + "]", ex);
                        // ignore malformed redirect
                    }
                }
            }
        }
    }

    public void setInferDOIs(boolean inferDOIs) {
        this.inferDOIs = inferDOIs;
    }

}
