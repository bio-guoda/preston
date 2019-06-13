package bio.guoda.preston.model;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.commons.rdf.simple.Types;
import bio.guoda.preston.DateUtil;

import java.net.URI;
import java.util.UUID;

import static bio.guoda.preston.RefNodeConstants.*;

public class RefNodeFactory {

    private static final RDF rdf = new SimpleRDF();
    private static final String SKOLEMIZATION_PATH = ".well-known/genid/";

    public static IRI fromUUID(String publisherUUID) {
        return toIRI(publisherUUID);
    }

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

    public static Literal toEnglishLiteral(String str) {
        return rdf.createLiteral(str, "en");
    }

    public static Literal toContentType(String contentType) {
        return RefNodeFactory.toLiteral(contentType);
    }

    public static Literal toDateTime(String dateTime) {
        return rdf.createLiteral(dateTime, Types.XSD_DATETIME);
    }

    public static boolean hasVersionStatement(Triple statement) {
        return HAS_VERSION.equals(statement.getPredicate())
                || HAS_PREVIOUS_VERSION.equals(statement.getPredicate());
    }

    public static IRI getVersionSource(Triple statement) {
        IRI versionSource = null;
        if (hasVersionStatement(statement)) {
            if (HAS_PREVIOUS_VERSION.equals(statement.getPredicate())) {
                RDFTerm object = statement.getObject();
                versionSource = object instanceof IRI ? (IRI) object : null;
            } else if (HAS_VERSION.equals(statement.getPredicate())) {
                RDFTerm subject = statement.getSubject();
                versionSource = subject instanceof IRI ? (IRI) subject : null;
            }
        }
        return versionSource;
    }

    public static BlankNodeOrIRI getVersion(Triple statement) {
        BlankNodeOrIRI version = null;
        if (hasVersionStatement(statement)) {
            if (HAS_PREVIOUS_VERSION.equals(statement.getPredicate())) {
                version = statement.getSubject();
            } else if (HAS_VERSION.equals(statement.getPredicate())) {
                RDFTerm object = statement.getObject();
                version = object instanceof BlankNodeOrIRI ? (BlankNodeOrIRI) object : null;
            }
        }
        return version;
    }

    public static Triple toStatement(BlankNodeOrIRI subject, IRI predicate, RDFTerm object) {
        return rdf.createTriple(subject, predicate, object);
    }

    public static BlankNode toBlank(String name) {
        return rdf.createBlankNode(name);
    }

    public static BlankNode toBlank() {
        return rdf.createBlankNode();
    }

    public static boolean isBlankOrSkolemizedBlank(BlankNodeOrIRI iri) {
        //see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return iri != null &&
                (iri instanceof BlankNode || isSkolemizedBlank(iri));
    }

    public static boolean isSkolemizedBlank(BlankNodeOrIRI iri) {
        //see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return iri.toString().contains(SKOLEMIZATION_PATH);
    }

    public static IRI toSkolemizedBlank(BlankNode subj) {
        // see https://www.w3.org/TR/rdf11-concepts/#section-skolemization
        return toIRI("https://deeplinker.bio/" + SKOLEMIZATION_PATH + subj.uniqueReference());
    }

    public static boolean hasVersionAvailable(Triple statement) {
        return hasVersionStatement(statement)
                && !isBlankOrSkolemizedBlank(getVersion(statement));
    }

    public static Literal nowDateTimeLiteral() {
        return toDateTime(DateUtil.now());
    }
}
