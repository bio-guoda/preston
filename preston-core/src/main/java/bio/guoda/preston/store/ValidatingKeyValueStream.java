package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public interface ValidatingKeyValueStream {

    InputStream getValueStream();

    boolean acceptValueStreamForKey(IRI key);
}
