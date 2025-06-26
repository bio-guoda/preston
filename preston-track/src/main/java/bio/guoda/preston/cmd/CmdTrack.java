package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerCachingProxy;
import bio.guoda.preston.store.DereferencerContentAddressed;
import bio.guoda.preston.store.HashKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class CmdTrack extends CmdActivity {
    private static final Logger LOG = LoggerFactory.getLogger(CmdTrack.class);
    public static final Pattern PREFIX_SCHEMA_PATTERN = Pattern.compile(HashKeyUtil.PREFIX_SCHEMA + ".*");
    public static final String DESCRIPTION_DEFAULT = "A crawl event that tracks digital content.";

    private Dereferencer<InputStream> dereferencer = ResourcesHTTP::asInputStream;

    @CommandLine.Parameters(
            description = "[url1] [url2] ..."
    )
    private List<IRI> IRIs = new ArrayList<>();

    @CommandLine.Option(
            names = {"-f", "--file"},
            description = "Read URLs to be tracked from file."
    )
    private String filename = null;


    @Override
    public String getDescriptionDefault() {
        return DESCRIPTION_DEFAULT;
    }

    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        if (StringUtils.isNotBlank(getFilename())) {
            if (!new File(getFilename()).exists()) {
                throw new RuntimeException("File [" + getFilename() + "] does not exist.");
            }
        }
        IRIs.forEach(iri -> {
            Quad quad = toStatement(ctx.getActivity(), iri, HAS_VERSION, toBlank());
            statementQueue.add(Collections.singletonList(quad));
        });
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {
        StatementsListener processor = createActivityProcessor(blobStore, ctx, listeners);

        handleQueue(statementQueue, processor);

        if (StringUtils.isNotBlank(getFilename())) {
            try (Stream<String> lines = Files.lines(Paths.get(new File(getFilename()).toURI()), StandardCharsets.UTF_8)) {
                lines
                        .map(expandToFileURIIfNeeded())
                        .map(RefNodeFactory::toIRI)
                        .map(iri -> toStatement(ctx.getActivity(), iri, HAS_VERSION, toBlank()))
                        .forEach(quad -> {
                            processor.on(quad);
                            // process derived statements also
                            handleQueue(statementQueue, processor);
                        });
            } catch (IOException e) {
                String msg = "failed to handle [" + getFilename() + "]";
                LOG.warn(msg, e);
                throw new RuntimeException(msg, e);
            }

            handleQueue(statementQueue, processor);

        }
    }

    private static void handleQueue(Queue<List<Quad>> statementQueue, StatementsListener processor) {
        while (!statementQueue.isEmpty()) {
            processor.on(statementQueue.poll());
        }
    }

    private Function<String, String> expandToFileURIIfNeeded() {
        return locationCandidate -> PREFIX_SCHEMA_PATTERN.matcher(locationCandidate).matches()
                ? locationCandidate
                : new File(locationCandidate).toPath().toUri().toString();
    }

    private StatementsListener createActivityProcessor(
            BlobStore blobStore,
            ActivityContext ctx,
            StatementsListener[] listeners) {
        return new Archiver(
                new DereferencerCachingProxy(new DereferencerContentAddressed(getDereferencer(), blobStore)),
                ctx,
                listeners);
    }

    protected Dereferencer<InputStream> getDereferencer() {
        return dereferencer;
    }

    protected void setDereferencer(Dereferencer<InputStream> dereferencer) {
        this.dereferencer = dereferencer;
    }

    public List<IRI> getIRIs() {
        return IRIs;
    }

    public void setIRIs(List<IRI> IRIs) {
        this.IRIs = IRIs;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }
}
