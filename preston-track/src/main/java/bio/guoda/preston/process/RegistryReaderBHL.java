package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderBHL extends ProcessorReadOnly {
    public static final String BHL_API_URL_PART = "//www.biodiversitylibrary.org/data/item.txt";
    public static final String BHL_DATASET_REGISTRY_STRING = "https:" + BHL_API_URL_PART;
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderBHL.class);
    public static final IRI BHL_REGISTRY = toIRI(BHL_DATASET_REGISTRY_STRING);

    public RegistryReaderBHL(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (Seeds.BHL.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream<Quad> nodes = Stream.of(
                    toStatement(Seeds.BHL, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a list of items (aka volumes) of works included in the Biodiversity Heritage Library.")),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, CREATED_BY, Seeds.BHL),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.TSV)),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, HAS_VERSION, toBlank())
            );
            ActivityUtil.emitAsNewActivity(nodes, this, statement.getGraphName());
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(BHL_API_URL_PART)) {
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
                    }, in, version);
                }
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    static void parse(StatementsEmitter emitter, InputStream in, IRI version) throws IOException {
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String header = bufferedReader.readLine();
        String[] columnNames = StringUtils.split(header, '\t');
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = StringUtils.trim(columnNames[i]);
            if ("BarCode".equals(columnName)) {
                handleBarCodes(bufferedReader, i, emitter, version);
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
