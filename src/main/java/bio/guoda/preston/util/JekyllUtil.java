package bio.guoda.preston.util;

import bio.guoda.preston.DateUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerTSV;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.joda.time.DateTime;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;

public class JekyllUtil {
    public static StatementsListener createJekyllSiteGenerator(BlobStoreReadOnly store, File jekyllDir) throws IOException {

        final String subdir = "_layouts";
        final List<String> filenames = Stream.of(RecordType.mediarecord, RecordType.record, RecordType.recordset)
                .map(x -> x.name() + ".html")
                .collect(Collectors.toList());
        copyStatic(jekyllDir, subdir, filenames);


        copyStatic(jekyllDir, "_includes", Collections.singletonList("media.html"));
        copyStatic(jekyllDir, "", Arrays.asList("index.md", "index.json"));

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

    public static void copyStatic(File jekyllDir, String subdir, List<String> filenames) throws IOException {

        final File resourceFolder =
                StringUtils.isBlank(subdir)
                ? jekyllDir :
                new File(jekyllDir, subdir);

        final String resourceFolderString = StringUtils.isBlank(subdir) ? "" : (subdir + "/");
        final String prefix = "/bio/guoda/preston/jekyll/" + resourceFolderString;

        for (String filename : filenames) {
            final File destination = new File(resourceFolder, filename);
            FileUtils.forceMkdirParent(destination);
            FileUtils.copyToFile(
                    JekyllUtil.class.getResourceAsStream(prefix + filename),
                    destination);
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
                final ObjectNode jekyllFrontMatterNode = objectMapper.createObjectNode();
                jekyllFrontMatterNode.put("layout", pageType.name());
                jekyllFrontMatterNode.put("id", uuid);
                jekyllFrontMatterNode.put("permalink", "/" + uuid);
                jekyllFrontMatterNode.set("idigbio", item);
                YAMLFactory jsonFactory = new YAMLFactory();
                jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
                final YAMLMapper yamlMapper = new YAMLMapper(jsonFactory);
                yamlMapper.writeValue(os, jekyllFrontMatterNode);
                org.apache.commons.io.IOUtils.write("---\n", os, StandardCharsets.UTF_8);
            }
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
                if (startTimeTerm instanceof Literal && activityId instanceof IRI) try {
                    final String lexicalForm = startTimeTerm.toString();
                    final String s = StringUtils.replace(lexicalForm, "\"^^<http://www.w3.org/2001/XMLSchema#dateTime>", "");
                    final DateTime startTime = DateUtil.parse(StringUtils.substring(s, 1));
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
