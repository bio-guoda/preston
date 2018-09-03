package org.globalbioticinteractions.preston.model;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.commons.rdf.simple.Types;
import org.globalbioticinteractions.preston.RefNodeConstants;

import java.net.URI;

public class RefNodeFactory {

    private static final RDF rdf = new SimpleRDF();
    public static final String SKOLEMIZATION_PATH = ".well-known/genid/";

    public static IRI toUUID(String publisherUUID) {
        return toIRI(publisherUUID);
    }

    public static IRI toIRI(String urlString) {
        return rdf.createIRI(urlString);
    }

    public static IRI toIRI(URI uri) {
        return rdf.createIRI(uri.toString());
    }

    public static Literal toLiteral(String bla) {
        return rdf.createLiteral(bla);
    }

    public static Literal toContentType(String contentType) {
        return RefNodeFactory.toLiteral(contentType);
    }

    public static Literal toDateTime(String dateTime) {
        return rdf.createLiteral(dateTime, Types.XSD_DATETIME);
    }

    public static boolean hasVersionOrIsDerivedFrom(Triple statement) {
        return hasVersionStatement(statement);
    }

    private static boolean hasVersionStatement(Triple statement) {
        return RefNodeConstants.WAS_DERIVED_FROM.equals(statement.getPredicate())
                || RefNodeConstants.HAS_VERSION.equals(statement.getPredicate())
                || RefNodeConstants.WAS_REVISION_OF.equals(statement.getPredicate())
                || RefNodeConstants.HAS_PREVIOUS_VERSION.equals(statement.getPredicate());
    }

    public static IRI getVersionSource(Triple statement) {
        IRI versionSource = null;
        if (hasVersionOrIsDerivedFrom(statement)) {
            if (RefNodeConstants.WAS_DERIVED_FROM.equals(statement.getPredicate())) {
                versionSource = (IRI) statement.getObject();
            } else {
                versionSource = (IRI) statement.getSubject();
            }
        }
        return versionSource;
    }

    public static BlankNodeOrIRI getVersion(Triple statement) {
        BlankNodeOrIRI version = null;
        if (hasVersionStatement(statement)) {
            if (RefNodeConstants.WAS_DERIVED_FROM.equals(statement.getPredicate())) {
                version = statement.getSubject();
            } else {
                version = (BlankNodeOrIRI) statement.getObject();
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

    public static boolean hasDerivedContentAvailable(Triple statement) {
        return hasVersionOrIsDerivedFrom(statement)
                && !isBlankOrSkolemizedBlank(getVersion(statement));
    }
}
