package bio.guoda.preston.stream;

import bio.guoda.preston.process.ProcessorStateReadOnly;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public interface ContentStreamHandler extends ProcessorStateReadOnly {

    boolean handle(IRI version, InputStream in) throws ContentStreamException;

}
