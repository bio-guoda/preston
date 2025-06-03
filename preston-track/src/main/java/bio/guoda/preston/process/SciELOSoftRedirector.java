package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class SciELOSoftRedirector extends ProcessorReadOnly {
    public static final String SCI_ELO_URL_PART = "scielo.php?script=sci_pdf";
    private static final Logger LOG = LoggerFactory.getLogger(SciELOSoftRedirector.class);

    public SciELOSoftRedirector(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
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
                    parse(new StatementsEmitterAdapter() {
                              @Override
                              public void emit(Quad statement) {
                                  nodes.add(statement);
                              }
                          },
                            in,
                            getVersionSource(statement)
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

    static void parse(StatementsEmitter emitter, InputStream ubin, IRI source) throws IOException {

        BufferedInputStream in = IOUtils.buffer(ubin);

        Metadata metadata = new Metadata();
        MediaType detectedType = new TextDetector().detect(in, metadata);

        if (MediaType.TEXT_PLAIN.equals(detectedType)) {
            LineIterator lineIterator = IOUtils.lineIterator(in, StandardCharsets.UTF_8);
            while (lineIterator.hasNext()) {
                String firstChunk = lineIterator.nextLine();
                Pattern redirectUrlPattern = Pattern.compile(".*<script>.*setTimeout.*window.location=\\\"(?<pdfUrl>.*)\\\".*</script>.*", Pattern.MULTILINE);
                Matcher matcher = redirectUrlPattern.matcher(firstChunk);
                if (matcher.matches()) {
                    String pdfUrl = matcher.group("pdfUrl");
                    try {
                        emitter.emit(RefNodeFactory.toStatement(source, SEE_ALSO, RefNodeFactory.toIRI(pdfUrl)));
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

    private static void handleBarCodes(BufferedReader bufferedReader, int barCodeIndex, StatementsEmitter emitter, IRI versionSource) throws IOException {
        String line;
        long lineNumber = 1;
        while ((line = bufferedReader.readLine()) != null) {
            lineNumber++;
            String[] values = StringUtils.split(line, '\t');
            if (barCodeIndex < values.length - 1) {
                String barCode = StringUtils.trim(values[barCodeIndex]);
                if (StringUtils.isNotBlank(barCode)) {
                    Stream.of(
                                    toStatement(toIRI(barCode),
                                            WAS_DERIVED_FROM,
                                            RefNodeFactory.toIRI("line:" + versionSource.getIRIString() + "!/L" + lineNumber)))

                            .forEach(emitter::emit);
                    submit(emitter, barCode, "_meta.xml", MimeTypes.XML);
                    submit(emitter, barCode, "_djvu.txt", MimeTypes.TEXT_UTF8);
                }
            }

        }
    }

    private static void submit(StatementsEmitter emitter, String barCode, String ext, String fileFormat) {
        IRI resource = toIRI("https://archive.org/download/" + barCode + "/" + barCode + ext);
        Stream.of(
                        toStatement(toIRI(barCode), SEE_ALSO, resource),
                        toStatement(resource, HAS_FORMAT, toContentType(fileFormat)),
                        toStatement(resource, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }


}
