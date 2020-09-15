package bio.guoda.preston.util;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.Preston;
import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.VersionUtil;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.commons.io.FileUtils;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;

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
                    final RDFTerm resource = statement.getObject();
                    if (resource instanceof IRI) {
                        try {
                            final InputStream is = store.get((IRI) resource);
                            if (is == null) {
                                throw new IOException("resource [" + ((IRI) resource).getIRIString() + "] needed for page generation, but not found");
                            }
                            writePages(is, uuid -> {
                                try {
                                    final File subdir = new File(posts, uuid.toString().substring(0, 2));
                                    final File destination = new File(subdir, uuid.toString() + ".md");
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
            }
        }
        return type;
    }

    public static void writePages(InputStream is, JekyllPageFactory factory, RecordType pageType) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JsonNode jsonNode = objectMapper.readTree(is);
        writePage(factory, pageType, objectMapper, jsonNode);
        if (jsonNode.has("items")) {
            for (JsonNode item : jsonNode.get("items")) {
                writePage(factory, pageType, objectMapper, item);
            }
        }
    }

    public static void writePage(JekyllPageFactory factory, RecordType pageType, ObjectMapper objectMapper, JsonNode item) throws IOException {
        if (item.has("uuid")) {
            final String uuid = item.get("uuid").asText();
            try (final OutputStream os = factory.outputStreamFor(UUID.fromString(uuid))) {
                final ObjectNode frontMatter = objectMapper.createObjectNode();
                frontMatter.put("layout", pageType.name());
                frontMatter.put("id", uuid);
                frontMatter.put("permalink", "/" + uuid);
                frontMatter.set("idigbio", item);

                writeFrontMatter(os, frontMatter);
            }
        }
    }

    static void writeFrontMatter(OutputStream os, ObjectNode jekyllFrontMatterNode) throws IOException {
        YAMLFactory jsonFactory = new YAMLFactory();
        jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        final ObjectWriter writer = new YAMLMapper(jsonFactory)
                .writer(new DefaultPrettyPrinter().withObjectIndenter(new DefaultIndenter().withLinefeed("\n")));
        writer.writeValue(os, jekyllFrontMatterNode);
        org.apache.commons.io.IOUtils.write("---\n", os, StandardCharsets.UTF_8);
    }

    static Stream<String> staticFileTemplates() {
        Reflections reflections = new Reflections("bio.guoda.preston.jekyll", new ResourcesScanner());
        Set<String> resourceList = reflections.getResources(
                Pattern.compile(".*\\.((md)|(json)|(html)|(png))"));
        return resourceList.stream().map(x -> "/" + x);
    }

    public static void writeVersionFile(File target, AtomicReference<DateTime> lastCrawlTime, StatementStore statementStore, IRI provenanceRoot) {
        final File data = new File(new File(target, "_data"), "version.yml");
        try {
            FileUtils.forceMkdirParent(data);
            try (final FileOutputStream out = new FileOutputStream(data)) {
                final IRI mostRecentVersion = VersionUtil.findMostRecentVersion(provenanceRoot, statementStore);
                final YAMLMapper yamlMapper = new YAMLMapper();
                final ObjectNode objectNode = yamlMapper.createObjectNode();
                objectNode.put("archive", mostRecentVersion.getIRIString());
                objectNode.put("preston", Preston.getVersion());
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
        unknown
    }

    public interface JekyllPageFactory {
        OutputStream outputStreamFor(UUID uuid);
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
