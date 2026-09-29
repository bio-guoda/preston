package bio.guoda.preston.stream;

import bio.guoda.preston.process.ProcessorStateReadOnly;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public interface ContentStreamHandlerInterface extends ProcessorStateReadOnly {

    boolean handle(IRI version, InputStream in, IRI source) throws ContentStreamException;

}
