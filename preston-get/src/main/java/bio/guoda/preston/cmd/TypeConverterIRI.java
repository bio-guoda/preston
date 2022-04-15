package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

public class TypeConverterIRI implements CommandLine.ITypeConverter<IRI> {

    @Override
    public IRI convert(String value) throws Exception {
        return RefNodeFactory.toIRI(value);
    }
}
