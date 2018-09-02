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
import org.globalbioticinteractions.preston.store.Predicate;

import java.net.URI;

public class RefNodeFactory {

    private static final RDF rdf = new SimpleRDF();


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

    public static boolean isDerivedFrom(Triple statement) {
        return statement.getSubject() != null
                && statement.getObject() != null
                && statement.getPredicate() != null
                && (Predicate.WAS_DERIVED_FROM.equals(statement.getPredicate())
                || Predicate.WAS_REVISION_OF.equals(statement.getPredicate()));
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

}
