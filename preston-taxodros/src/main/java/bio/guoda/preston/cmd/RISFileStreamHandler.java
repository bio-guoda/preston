package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RISFileStreamHandler implements ContentStreamHandler {
    public static final String ZOTERO_JOURNAL_ARTICLE = "journalArticle";
    public static final String ZOTERO_BOOK = "book";
    public static final String ZOTERO_BOOK_SECTION = "bookSection";
    public static final String ZOTERO_PREPRINT = "preprint";
    public static final String ZOTERO_REPORT = "report";
    public static final String ZOTERO_THESIS = "thesis";
    public static final String ZOTERO_CONFERENCE_PAPER = "conferencePaper";
    private final Logger LOG = LoggerFactory.getLogger(RISFileStreamHandler.class);


    private final Persisting persisting;
    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private final List<String> communities;

    public RISFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                OutputStream os,
                                Persisting persisting,
                                Dereferencer<InputStream> dereferencer,
                                List<String> communities) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.persisting = persisting;
        this.dereferencer = dereferencer;
        this.communities = communities;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            RISUtil.parseRIS(is, new Consumer<ObjectNode>() {
                @Override
                public void accept(ObjectNode jsonNode) {
                    try {
                        StreamHandlerUtil.writeRecord(foundAtLeastOne, RISUtil.translateRISToZenodo(jsonNode), outputStream);
                    } catch (IOException e) {
                        //
                    }
                }
            }, iriString);

        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        }
        return foundAtLeastOne.get();
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
