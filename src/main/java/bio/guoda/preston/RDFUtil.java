package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.lang.RiotParsers;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.FactoryRDF;
import org.apache.jena.riot.system.IRIResolver;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jena.riot.system.RiotLib.createParserProfile;
import static org.apache.jena.riot.system.RiotLib.factoryRDF;

public class RDFUtil {
    public static String getValueFor(RDFTerm entity) {
        String value = null;
        if (entity instanceof IRI) {
            value = ((IRI) entity).getIRIString();
        } else if (entity instanceof Literal){
            value = ((Literal) entity).getLexicalForm();
        }
        value = (value == null) ? entity.toString() : value;
        return value;
    }

    public static Iterator<Quad> asQuads(InputStream inputStream) {
        FactoryRDF factory = factoryRDF(LabelToNode.createScopeByGraph());
        ParserProfile profile = createParserProfile(factory, ErrorHandlerFactory.errorHandlerStd, false);
        profile.setIRIResolver(IRIResolver.createNoResolve());
        return RiotParsers.createIteratorNQuads(inputStream, (StreamRDF) null, profile);
    }

    public static Stream<Quad> asQuadStream(InputStream inputStream) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(asQuads(inputStream), Spliterator.ORDERED), false)
    }
}
