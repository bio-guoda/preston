package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.txt.UniversalEncodingDetector;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.stream.ContentStreamFactory.URI_PREFIX_LINE;

public class LineStreamHandler implements ContentStreamHandler {

    private final ContentStreamHandler contentStreamHandler;

    public LineStreamHandler(ContentStreamHandler contentStreamHandler) {
        this.contentStreamHandler = contentStreamHandler;
    }

    @Override
    public boolean handle(IRI version, InputStream in) throws ContentStreamException {
        if (!version.getIRIString().startsWith(URI_PREFIX_LINE + ":")) {
            Charset charset;
            try {
                charset = new UniversalEncodingDetector().detect(in, new Metadata());
            } catch (IOException e) {
                throw new ContentStreamException("failed to detect charset", e);
            }
            if (charset != null) {
                try {
                    extractLines(version, in, charset);
                } catch (IOException e) {
                    throw new ContentStreamException("failed to parse text", e);
                }
                return true;
            }
        }
        return false;
    }

    private void extractLines(IRI version, InputStream in, Charset charset) throws ContentStreamException, IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset));

        for (int lineNumber = 1; contentStreamHandler.shouldKeepProcessing(); ++lineNumber) {
            String string = reader.readLine();
            if (string == null) {
                break;
            } else {
                contentStreamHandler.handle(
                        getLineIri(version, lineNumber),
                        new ByteArrayInputStream(string.getBytes(charset))
                );
            }
        }
    }

    private IRI getLineIri(IRI version, int lineNumber) {
        return toIRI(String.format((URI_PREFIX_LINE + ":") + "%s!/L%d", version.getIRIString(), lineNumber));
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }

}
