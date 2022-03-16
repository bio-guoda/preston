package bio.guoda.preston;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.lang.RiotParsers;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.FactoryRDF;
import org.apache.jena.riot.system.ParserProfile;
import org.apache.jena.sparql.core.Quad;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.jena.riot.system.RiotLib.createParserProfile;
import static org.apache.jena.riot.system.RiotLib.factoryRDF;

public class RDFUtil {

    public static final RDF RDF_FACTORY = new JenaRDF();

    public static String getValueFor(RDFTerm entity) {
        return RDFValueUtil.getValueFor(entity);
    }

    public static org.apache.commons.rdf.api.Quad asQuad(String string) {
        Iterator<Quad> quadIterator = asQuads(IOUtils.toInputStream(string, StandardCharsets.UTF_8));
        if (quadIterator.hasNext()) {
            try {
                return JenaRDF.asQuad(RDF_FACTORY, quadIterator.next());
            } catch (RiotException exception) {
                return null;
            }
        } else {
            return null;
        }
    }

    public static Iterator<Quad> asQuads(InputStream inputStream) {
        ARQ.init();
        FactoryRDF factory = factoryRDF(LabelToNode.createScopeByGraph());
        ParserProfile profile = createParserProfile(
                factory,
                ErrorHandlerFactory.errorHandlerStd,
                false);

        profile.setBaseIRI("urn:example:");
        return RiotParsers.createIteratorNQuads(inputStream, null, profile);
    }

    public static Stream<org.apache.commons.rdf.api.Quad> asQuadStream(InputStream inputStream) {
        return StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(asQuads(inputStream), Spliterator.ORDERED), false)
                .map(q -> JenaRDF.asQuad(RDF_FACTORY, q));
    }
}
