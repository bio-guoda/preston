package bio.guoda.preston.util;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.Preston;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ValueListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.VersionUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.joda.time.DateTime;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;

public class JekyllUtil {
    public static StatementsListener createJekyllSiteGenerator(BlobStoreReadOnly store, File jekyllDir) throws IOException {

        copyStatic(jekyllDir);

        final File posts = new File(jekyllDir, "pages");
        try {
            FileUtils.forceMkdir(posts);
        } catch (IOException ex) {
            throw new IOException("failed to create jekyll [posts/] dir at [" + posts.getAbsolutePath() + "]", ex);
        }


        final File jekyllData = new File(jekyllDir, "_data");
        FileUtils.forceMkdir(jekyllData);
        final File contentVersions = new File(jekyllData, "content.tsv");
        if (contentVersions.exists()) {
            FileUtils.deleteQuietly(contentVersions);
        }
        final PrintStream printWriter = new PrintStream(new FileOutputStream(contentVersions));
        printWriter.println("url\tverb\thash\tgraphname");
        final StatementListener versionLogger = new StatementLoggerTSV(printWriter);
        return createPageGenerators(store, posts, versionLogger);
    }

    public static void copyStatic(File jekyllDir) throws IOException {
        final Stream<String> strings = staticFileTemplates();

        final List<Pair<String, File>> collect = strings.map(x -> {
            final String filename = StringUtils.replace(x, "/bio/guoda/preston/jekyll/", "");
            return Pair.of(x, new File(jekyllDir, filename));
        }).collect(Collectors.toList());

        for (Pair<String, File> stringFilePair : collect) {
            FileUtils.copyToFile(JekyllUtil.class.getResourceAsStream(stringFilePair.getKey()), stringFilePair.getValue());
        }

        FileUtils.copyToFile(IOUtils.toInputStream(
                "_config.yml\n" +
                        "_site\n" +
                        "tmp/\n" +
                        ".jekyll-cache", StandardCharsets.UTF_8), new File(jekyllDir, ".gitignore"));
    }

    public static StatementsListener createPageGenerators(BlobStoreReadOnly store, File posts, StatementListener contentVersionLogger) {
        return new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                if (RefNodeFactory.hasVersionAvailable(statement)) {
                    contentVersionLogger.on(statement);
                }

                final RecordType recordType = guessRecordType(statement);


                if (!RecordType.unknown.equals(recordType)) {
                    JekyllPageWriter pageWriter = writerForRecordType(recordType);

                    final RDFTerm resource = statement.getObject();
                    if (resource instanceof IRI) {
                        try {
                            final InputStream is = store.get((IRI) resource);
                            if (is == null) {
                                throw new IOException("resource [" + ((IRI) resource).getIRIString() + "] needed for page generation, but not found");
                            }
                            pageWriter.writePages(is, iri -> {
                                try {
                                    String uuid = iri.getIRIString()
                                            .replaceFirst("urn:uuid:", "")
                                            .replaceFirst(".*/occurrence/", "");
                                    final File subdir = new File(posts, uuid.substring(0, 2));
                                    final File destination = new File(subdir, uuid + ".md");
                                    FileUtils.forceMkdirParent(destination);
                                    return new FileOutputStream(destination);
                                } catch (IOException e) {
                                    throw new RuntimeException("failed to generate jekyll pages", e);
                                }
                            }, recordType);

                        } catch (IOException e) {
                            throw new RuntimeException("failed to create jekyll pages", e);
                        }
                    }
                }

            }

            private JekyllPageWriter writerForRecordType(RecordType recordType) {
                JekyllPageWriter pageWriter = new JekyllPageWriterIDigBio();
                if (RecordType.occurrence.equals(recordType)) {
                    pageWriter = new JekyllPageWriterGBIF();
                }
                return pageWriter;
            }

        };
    }

    public static RecordType guessRecordType(Quad statement) {
        RecordType type = RecordType.unknown;
        if (hasVersionAvailable(statement)) {
            final BlankNodeOrIRI version = statement.getSubject();
            if (RegistryReaderIDigBio.isRecordSetSearchEndpoint(version)) {
                type = RecordType.recordset;
            } else if (RegistryReaderIDigBio.isRecordSetViewEndpoint(version)) {
                type = RecordType.recordset;
            } else if (RegistryReaderIDigBio.isMediaRecordEndpoint(version)) {
                type = RecordType.mediarecord;
            } else if (RegistryReaderIDigBio.isRecordsEndpoint(version)) {
                type = RecordType.record;
            } else if (RegistryReaderGBIF.isOccurrenceRecordEndpoint(version)) {
                type = RecordType.occurrence;
            }
        }
        return type;
    }

    static void writeFrontMatter(OutputStream os, ObjectNode jekyllFrontMatterNode) throws IOException {
        YAMLFactory jsonFactory = new YAMLFactory();
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        final ObjectWriter writer = new YAMLMapper(jsonFactory)
                .writer(new DefaultPrettyPrinter()
                        .withObjectIndenter(new DefaultIndenter().withLinefeed("\n")));
        writer.writeValue(os, jekyllFrontMatterNode);
        org.apache.commons.io.IOUtils.write("---\n", os, StandardCharsets.UTF_8);
    }

    static Stream<String> staticFileTemplates() {
        Reflections reflections = new Reflections("bio.guoda.preston.jekyll", new ResourcesScanner());
        Set<String> resourceList = reflections.getResources(
                Pattern.compile(".*"));
        return resourceList.stream().map(x -> "/" + x);
    }

    public static void writePrestonConfigFile(File target, AtomicReference<DateTime> lastCrawlTime, HexaStore hexastore, IRI provenanceRoot) {
        final File data = new File(new File(target, "_data"), "preston.yml");
        try {
            FileUtils.forceMkdirParent(data);
            try (final FileOutputStream out = new FileOutputStream(data)) {
                final IRI mostRecentVersion = VersionUtil.findMostRecentVersion(provenanceRoot, hexastore);
                final YAMLMapper yamlMapper = new YAMLMapper();
                final ObjectNode objectNode = yamlMapper.createObjectNode();
                objectNode.put("archive", mostRecentVersion.getIRIString());
                objectNode.put("data_location_comment", "replace [data_location/provenance_location] if content is hosted separately, e.g., [https://example.org/data/] would link content to locations like [https://example.org/data/aa/bb/aabb...] where [aabb...] is a sha256 content/provenance hash");
                objectNode.put("data_location", "data/");
                objectNode.put("provenance_location", "data/");
                objectNode.put("version", Preston.getVersion());
                final DateTime dateTime = lastCrawlTime.get();
                if (dateTime != null) {
                    objectNode.put("created_at", dateTime.toString());
                }
                yamlMapper.writeValue(out, objectNode);
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to traverse version history", e);
        }
    }

    public enum RecordType {
        recordset,
        mediarecord,
        record,
        occurrence,
        unknown
    }

    public interface JekyllPageFactory {
        OutputStream outputStreamFor(IRI iri);
    }

    public static StatementsListener createPrestonStartTimeListener(ValueListener<DateTime> listener) {
        return new PrestonStartTimeListener(listener);
    }

    private static class PrestonStartTimeListener extends StatementsListenerAdapter {

        private final ValueListener<DateTime> listener;
        private AtomicReference<Pair<DateTime, IRI>> lastActivityStartTime;
        private AtomicReference<IRI> lastPrestonActivityId;

        PrestonStartTimeListener(ValueListener<DateTime> listener) {
            this.listener = listener;
            lastActivityStartTime = new AtomicReference<>();
            lastPrestonActivityId = new AtomicReference<>();
        }

        @Override
        public void on(Quad statement) {
            if (RefNodeConstants.STARTED_AT_TIME.equals(statement.getPredicate())) {
                final BlankNodeOrIRI activityId = statement.getSubject();
                final RDFTerm startTimeTerm = statement.getObject();
                try {
                    final DateTime startTime = DateUtil.parse(RDFUtil.getValueFor(startTimeTerm));
                    lastActivityStartTime.set(Pair.of(startTime, (IRI) activityId));
                } catch (IllegalArgumentException ex) {
                    //
                }
            } else if (RefNodeConstants.WAS_STARTED_BY.equals(statement.getPredicate())
                    && RefNodeConstants.PRESTON.equals(statement.getObject())
                    && statement.getSubject() instanceof IRI) {
                lastPrestonActivityId.set((IRI) statement.getSubject());
            }

            final Pair<DateTime, IRI> dateTimeIRIPair = lastActivityStartTime.get();
            if (dateTimeIRIPair != null && dateTimeIRIPair.getValue().equals(lastPrestonActivityId.get())) {
                listener.on(dateTimeIRIPair.getKey());
            }
        }
    }
}
