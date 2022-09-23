package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.util.List;

public interface ValidatingKeyValueStream {

    InputStream getValueStream();

    boolean acceptValueStreamForKey(IRI key);

    List<String> getViolations();
}
