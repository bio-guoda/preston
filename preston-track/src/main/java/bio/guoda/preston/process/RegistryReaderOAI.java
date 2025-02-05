package bio.guoda.preston.process;

import bio.guoda.preston.MimeTypes;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.getVersion;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.hasVersionAvailable;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toContentType;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class RegistryReaderOAI extends ProcessorReadOnly {
    private final Logger LOG = LoggerFactory.getLogger(RegistryReaderOAI.class);

    public RegistryReaderOAI(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (hasVersionAvailable(statement)
                && possiblyDescribesOAIRequest(statement)) {
            handleOAIResponse(statement);
        }
    }

    static boolean possiblyDescribesOAIRequest(Quad statement) {
        IRI versionSource = getVersionSource(statement);
        return StringUtils.contains(versionSource.toString(), "verb=")
                || StringUtils.contains(versionSource.toString(), "metadataPrefix=oai_");
    }

    public void handleOAIResponse(Quad statement) {
        List<Quad> nodes = new ArrayList<>();
        try {
            IRI currentPage = (IRI) getVersion(statement);
            InputStream is = get(currentPage);
            if (is != null) {
                parseOAIResultPage(new StatementsEmitterAdapter() {
                    @Override
                    public void emit(Quad statement) {
                        nodes.add(statement);
                    }
                }, is, getVersionSource(statement));
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.toString() + "]", e);
        }
        if (nodes.size() > 0) {
            ActivityUtil.emitAsNewActivity(nodes.stream(), this, statement.getGraphName());
        }
    }

    static void emitNextPage(String resumptionToken, StatementsEmitter emitter, String versionSourceURI) {
        String nextPageURL = versionSourceURI;
        Pattern verb = Pattern.compile("(?<prefix>.*)\\?verb=(?<verb>[a-zA-Z]+)(.*)");
        Matcher matcher = verb.matcher(nextPageURL);
        if (matcher.matches()) {
            nextPageURL = matcher.group("prefix")
                    + "?verb="
                    + matcher.group("verb")
                    + "&resumptionToken="
                    + resumptionToken;
            IRI nextPage = toIRI(nextPageURL);
            emitPageRequest(emitter, nextPage, MimeTypes.XML);
        }
    }

    private static void emitPageRequest(StatementsEmitter emitter, IRI nextPage, String mimeType) {
        Stream.of(
                toStatement(nextPage, HAS_FORMAT, toContentType(mimeType)),
                toStatement(nextPage, HAS_VERSION, toBlank()))
                .forEach(emitter::emit);
    }

    static void parseOAIResultPage(StatementsEmitter emitter, InputStream in, IRI versionSource) throws IOException {
        try {
            NodeList nodeList = XMLUtil.evaluateXPath("//*[local-name()='resumptionToken']", in);
            if (nodeList != null && nodeList.getLength() > 0) {
                Node item = nodeList.item(0);
                String resumptionToken = item.getTextContent();
                emitNextPageIfNeeded(emitter, versionSource, resumptionToken);
            }
        } catch (ParserConfigurationException | SAXException | XPathExpressionException e) {
            throw new IOException("failed to process suspected OAI response", e);
        }


    }

    private static void emitNextPageIfNeeded(StatementsEmitter emitter, IRI versionSource, String resumptionToken) {
        if (StringUtils.isNotBlank(resumptionToken)) {
            String previousURL = versionSource.getIRIString();
            emitNextPage(resumptionToken, emitter, previousURL);
        }
    }


}
