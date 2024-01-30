package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.DereferencerContentAddressed;
import bio.guoda.preston.store.DereferencerCachingProxy;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class CmdTrack extends CmdActivity {
    private static final Logger LOG = LoggerFactory.getLogger(CmdTrack.class);

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

        while (!statementQueue.isEmpty()) {
            processor.on(statementQueue.poll());
        }

        if (StringUtils.isNotBlank(getFilename())) {
            Stream<String> lines = null;
            try {
                lines = Files.lines(Paths.get(new File(getFilename()).toURI()), StandardCharsets.UTF_8);
                lines.map(RefNodeFactory::toIRI)
                        .map(iri -> toStatement(ctx.getActivity(), iri, HAS_VERSION, toBlank()))
                        .forEach(processor::on);
            } catch (IOException e) {
                String msg = "failed to handle [" + getFilename() + "]";
                LOG.warn(msg, e);
                throw new RuntimeException(msg, e);
            }
        }
    }

    @Override
    String getActivityDescription() {
        return "A crawl event that tracks digital content.";
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
