package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public abstract class ContentStreamHandler implements ContentStreamHandlerInterface {

    public boolean handle(IRI version, InputStream in, IRI source) throws ContentStreamException {
        return handle(version, in);
    }


}
