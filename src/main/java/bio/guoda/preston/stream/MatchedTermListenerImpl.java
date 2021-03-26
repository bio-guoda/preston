package bio.guoda.preston.stream;

import bio.guoda.preston.process.StatementEmitter;
import org.apache.commons.rdf.api.IRI;

import java.util.Map;
import java.util.regex.Matcher;

import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class MatchedTermListenerImpl implements MatchedTermListener {
    private final StatementEmitter emitter;
    private final Map<Integer, String> patternGroupNames;

    public MatchedTermListenerImpl(StatementEmitter emitter, Map<Integer, String> patternGroupNames) {
        this.emitter = emitter;
        this.patternGroupNames = patternGroupNames;
    }

    @Override
    public void onMatchedTerm(Matcher matcher, IRI matchIri, int captureGroupIndex, String captureGroup, IRI captureGroupIri) {
        emitter.emit(toStatement(captureGroupIri, HAS_VALUE, toLiteral(captureGroup)));

        if (captureGroupIndex > 0) {
            emitter.emit(toStatement(matchIri, HAD_MEMBER, captureGroupIri));

            if (patternGroupNames.containsKey(captureGroupIndex)) {
                String groupName = patternGroupNames.get(captureGroupIndex);
                if (captureGroup.equals(matcher.group(groupName))) {
                    emitter.emit(toStatement(matchIri, DESCRIPTION, toLiteral(groupName)));
                } else {
                    throw new RuntimeException("pattern group [" + groupName + "] was assigned the wrong index");
                }
            }
        }
    }
}
