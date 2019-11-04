package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;

public interface KeyValueStreamFactory {
    ValidatingKeyValueStream forKeyValueStream(IRI key, InputStream is);
}
