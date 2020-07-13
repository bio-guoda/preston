package bio.guoda.preston.model;

import bio.guoda.preston.DateUtil;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.commons.rdf.simple.Types;

import java.net.URI;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;

public class RefNodeFactory {

    private static final RDF rdf = new SimpleRDF();
    private static final String SKOLEMIZATION_PATH = ".well-known/genid/";

    public static IRI toIRI(String urlString) {
        return rdf.createIRI(urlString);
    }

    public static IRI toIRI(UUID uuid) {
        return rdf.createIRI(uuid.toString());
    }

    public static IRI toIRI(URI uri) {
        return rdf.createIRI(uri.toString());
    }

    public static Literal toLiteral(String str) {
        return rdf.createLiteral(str);
    }

    public static Literal toLiteral(String str, IRI dataType) {
        return rdf.createLiteral(str, dataType);
    }

    public static Literal toEnglishLiteral(String str) {
        return rdf.createLiteral(str, "en");
    }

    public static Literal toContentType(String contentType) {
        return RefNodeFactory.toLiteral(contentType);
    }

    public static Literal toDateTime(String dateTime) {
        return rdf.createLiteral(dateTime, Types.XSD_DATETIME);
    }

    public static boolean hasVersionStatement(Quad statement) {
        return HAS_VERSION.equals(statement.getPredicate())
                || HAS_PREVIOUS_VERSION.equals(statement.getPredicate());
    }

    public static IRI getVersionSource(Quad statement) {
        IRI versionSource = null;
        if (hasVersionStatement(statement)) {
            versionSource = (IRI) statement.getSubject();
        }
        return versionSource;
    }

    public static BlankNodeOrIRI getVersion(Quad statement) {
        BlankNodeOrIRI version = null;
        if (hasVersionStatement(statement)) {
            version = (BlankNodeOrIRI) statement.getObject();
        }
        return version;
    }

    public static Quad toStatement(BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {
        return rdf.createQuad(null, subject, predicate, object);
    }

    public static Quad toStatement(BlankNodeOrIRI graphName, Quad quad) {
        return rdf.createQuad(graphName, quad.getSubject(), quad.getPredicate(), quad.getObject());
    }
    public static Quad toStatement(BlankNodeOrIRI graphName, BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {
        return rdf.createQuad(graphName, subject, predicate, object);
    }

    public static BlankNode toBlank(String name) {
        return rdf.createBlankNode(name);
    }

    public static BlankNode toBlank() {
        return rdf.createBlankNode();
    }

    public static boolean isBlankOrSkolemizedBlank(RDFTerm iri) {
        //see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return iri != null &&
                (iri instanceof BlankNode || isSkolemizedBlank(iri));
    }

    public static boolean isSkolemizedBlank(RDFTerm iri) {
        //see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return iri.toString().contains(SKOLEMIZATION_PATH);
    }

    public static IRI toSkolemizedBlank(BlankNode subj) {
        // see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return toIRI("https://deeplinker.bio/" + SKOLEMIZATION_PATH + subj.uniqueReference());
    }

    public static boolean hasVersionAvailable(Quad statement) {
        return hasVersionStatement(statement)
                && !isBlankOrSkolemizedBlank(getVersion(statement));
    }

    public static Literal nowDateTimeLiteral() {
        return toDateTime(DateUtil.now());
    }

}
