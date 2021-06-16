package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;

import java.util.regex.Matcher;

public interface MatchedTermListener {
    void onMatchedTerm(Matcher matcher, IRI matchIri, int captureGroupIndex, String captureGroup, IRI captureGroupIri);
}
