package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.ORGANIZATION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderBHL extends ProcessorReadOnly {
    private static final Map<String, String> SUPPORTED_ENDPOINT_TYPES = new HashMap<String, String>() {{
        put("DWC_ARCHIVE", MimeTypes.MIME_TYPE_DWCA);
        put("EML", MimeTypes.MIME_TYPE_EML);
    }};


    public static final String BHL_API_URL_PART = "//www.biodiversitylibrary.org/data/item.txt";
    public static final String BHL_DATASET_REGISTRY_STRING = "https:" + BHL_API_URL_PART;
    private final Log LOG = LogFactory.getLog(RegistryReaderBHL.class);
    public static final IRI BHL_REGISTRY = toIRI(BHL_DATASET_REGISTRY_STRING);

    public RegistryReaderBHL(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Triple statement) {
        if (Seeds.BHL.equals(statement.getSubject())
                && WAS_ASSOCIATED_WITH.equals(statement.getPredicate())) {
            Stream.of(
                    toStatement(Seeds.BHL, IS_A, ORGANIZATION),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a list of items (aka volumes) of works included in the Biodiversity Heritage Library.")),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, CREATED_BY, Seeds.BHL),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, HAS_FORMAT, toContentType(MimeTypes.TSV)),
                    toStatement(RegistryReaderBHL.BHL_REGISTRY, HAS_VERSION, toBlank())
            ).forEach(this::emit);
        } else if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(BHL_API_URL_PART)) {
            try {
                IRI currentPage = (IRI) getVersion(statement);
                parse(this, get(currentPage), getVersionSource(statement));
            } catch (IOException e) {
                LOG.warn("failed to handle [" + statement.toString() + "]", e);
            }
        }
    }

    static void parse(StatementEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        InputStreamReader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader);
        String header = bufferedReader.readLine();
        String[] columnNames = StringUtils.split(header, '\t');
        for (int i = 0; i < columnNames.length; i++) {
            if ("BarCode".equals(StringUtils.trim(columnNames[i]))) {
                handleBarCodes(bufferedReader, i, emitter);
            }
        }
    }

    private static void handleBarCodes(BufferedReader bufferedReader, int i, StatementEmitter emitter) throws IOException {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] values = StringUtils.split(line, '\t');
            String barCode = StringUtils.trim(values[i]);
            if (StringUtils.isNotBlank(barCode)) {
                IRI ocrText = toIRI("https://archive.org/download/" + barCode + "/" + barCode + "_djvu.txt");
                Stream.of(
                        toStatement(ocrText, HAS_FORMAT, toContentType(MimeTypes.TEXT_UTF8)),
                        toStatement(ocrText, HAS_VERSION, toBlank()))
                        .forEach(emitter::emit);
            }

        }
    }


}
