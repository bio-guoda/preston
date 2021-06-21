package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import com.beust.jcommander.IStringConverter;
import org.apache.commons.rdf.api.IRI;

public class IRIConverter implements IStringConverter<IRI> {
    @Override
    public IRI convert(String value) {
        return RefNodeFactory.toIRI(value);
    }
}
