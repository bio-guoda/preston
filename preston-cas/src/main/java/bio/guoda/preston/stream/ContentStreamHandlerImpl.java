package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class ContentStreamHandlerImpl implements ContentStreamHandler {

    private final List<ContentStreamHandler> handlers;

    public ContentStreamHandlerImpl() {
        handlers = Arrays.asList(
                new ArchiveStreamHandler(this),
                new CompressedStreamHandler(this)
        );
    }

    public ContentStreamHandlerImpl(ContentStreamHandler... handler) {
        handlers = Arrays.asList(handler);
    }

    @Override
    public boolean handle(IRI version, InputStream in) throws ContentStreamException {
        InputStream markableInputStream = ContentStreamUtil.getMarkSupportedInputStream(in);

        boolean handled;

        for (ContentStreamHandler handler : handlers) {
            handled = handler.handle(version, markableInputStream);
            if (handled) {
                break;
            }
        }

        return true;
    }


    public static IRI wrapIRI(String prefix, IRI version, String suffix) throws URISyntaxException {
        URI uri = new URI(prefix, version.getIRIString() + (suffix != null ? "!/" + suffix : ""), null);
        return toIRI(uri);
    }

    public static IRI wrapIRI(String prefix, IRI version) throws URISyntaxException {
        return wrapIRI(prefix, version, null);
    }

    @Override
    public boolean shouldKeepReading() {
        return true;
    }

}
