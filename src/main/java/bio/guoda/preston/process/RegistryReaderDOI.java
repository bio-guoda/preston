package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.cxf.helpers.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static bio.guoda.preston.model.RefNodeFactory.getVersion;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toContentType;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class RegistryReaderDOI extends ProcessorReadOnly {


    public static final String GBIF_DOI_PART = "10.15468/";
    private final Log LOG = LogFactory.getLog(RegistryReaderDOI.class);

    public RegistryReaderDOI(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)
                && getVersionSource(statement).toString().contains(GBIF_DOI_PART)) {
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


        }
    }

    static void parseGBIFDownloadHtmlPage(Quad statement, InputStream is, StatementEmitter emitter) throws IOException {
        String htmlPage = IOUtils.toString(is, StandardCharsets.UTF_8.name());
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
