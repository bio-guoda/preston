package bio.guoda.preston.cmd;

import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

public class GenBankFlatFileStreamHandler implements ContentStreamHandler {

    private final Dereferencer<InputStream> dereferencer;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;

    public GenBankFlatFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                        Dereferencer<InputStream> inputStreamDereferencer,
                                        OutputStream os) {
        this.contentStreamHandler = contentStreamHandler;
        this.dereferencer = inputStreamDereferencer;
        this.outputStream = os;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            Charset charset = new UniversalEncodingDetector().detect(is, new Metadata());
            if (charset != null) {
                int lineStart = -1;
                int lineFinish = -1;
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));

                for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    } else {
                        if (StringUtils.startsWith(line, "LOCUS")) {
                            lineStart = lineNumber;
                        } else if (StringUtils.startsWith(line, "//")) {
                            lineFinish = lineNumber;
                        }

                        if (lineFinish > lineStart) {
                            ObjectNode objectNode = new ObjectMapper().createObjectNode();
                            objectNode.set("http://www.w3.org/ns/prov#wasDerivedFrom", TextNode.valueOf("line:" + iriString + "!/L" + lineStart + "-" + "L" + lineFinish));
                            objectNode.set("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", TextNode.valueOf("genbank-flatfile"));
                            IOUtils.copy(IOUtils.toInputStream(objectNode.toString(), StandardCharsets.UTF_8), outputStream);
                            IOUtils.copy(IOUtils.toInputStream("\n", StandardCharsets.UTF_8), outputStream);
                            lineStart = -1;
                            lineFinish = -1;
                            foundAtLeastOne.set(true);
                        }
                    }
                }
            }

        } catch (IOException e) {
            throw new ContentStreamException("no charset detected");
        }

        return foundAtLeastOne.get();
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
