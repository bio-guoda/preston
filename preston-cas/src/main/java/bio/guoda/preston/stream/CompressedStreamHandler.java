package bio.guoda.preston.stream;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.net.URISyntaxException;

import static bio.guoda.preston.stream.ContentStreamHandlerImpl.wrapIRI;

public class CompressedStreamHandler implements ContentStreamHandler {

    private ContentStreamHandler contentStreamHandler;

    public CompressedStreamHandler(ContentStreamHandler contentStreamHandler) {
        this.contentStreamHandler = contentStreamHandler;
    }

    @Override
    public boolean handle(IRI version, InputStream in) throws ContentStreamException {
        Pair<CompressorInputStream, String> compressedStreamAndFormat = getCompressedStreamAndFormat(in);
        if (compressedStreamAndFormat != null) {
            parseAsCompressed(version, compressedStreamAndFormat.getLeft(), compressedStreamAndFormat.getRight());
            return true;
        }
        return false;
    }

    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }

    private Pair<CompressorInputStream, String> getCompressedStreamAndFormat(InputStream in) {
        try {
            String compressionFormat = CompressorStreamFactory.detect(in);
            // do not close this stream; it would also close the "in" stream
            CompressorInputStream compressedInputStream = new CompressorStreamFactory()
                    .createCompressorInputStream(in);
            return Pair.of(compressedInputStream, compressionFormat);
        } catch (CompressorException e) {
            return null;
        }
    }



    protected void parseAsCompressed(IRI version, InputStream in, String compressionFormat) throws ContentStreamException {
        try {
            contentStreamHandler.handle(wrapIRI(compressionFormat, version), in);
        } catch (URISyntaxException e) {
            throw new ContentStreamException("failed to create content URI", e);
        }
    }

}
