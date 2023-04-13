package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.cmd.CopyShopNQuadToTSV;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Matcher;

public class EmittingStreamOfAnyQuad extends EmittingStreamAbstract {

    public static final String DEFAULT_PREFIX_X_PRESTON = "x:preston:";

    public EmittingStreamOfAnyQuad(StatementEmitter emitter) {
        super(emitter);
    }

    public EmittingStreamOfAnyQuad(StatementEmitter emitter, ProcessorState processorState) {
        super(emitter, processorState);
    }

    @Override
    public void parseAndEmit(InputStream is) {
        if (!getContext().shouldKeepProcessing()) {
            throw new RDFHandlerException("stop processing");
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        try {
            while (getContext().shouldKeepProcessing() && (line = reader.readLine()) != null) {
                Matcher matcher = CopyShopNQuadToTSV.WITH_IRI_OBJECT_WITH_NAMESPACE.matcher(line);
                if (matcher.matches()) {
                    emitQuadWithIRIObject(matcher);
                } else {
                    matcher = CopyShopNQuadToTSV.WITH_IRI_OBJECT_WITHOUT_NAMESPACE.matcher(line);
                    if (matcher.matches()) {
                        emitQuadWithIRIObjectNoNamespace(matcher);
                    } else {
                        matcher = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT_WITH_NAMESPACE.matcher(line);
                        if (matcher.matches()) {
                            emitQuadWithLiteralObject(matcher);
                        } else {
                            matcher = CopyShopNQuadToTSV.WITH_LITERAL_OBJECT_WITHOUT_NAMESPACE.matcher(line);
                            if (matcher.matches()) {
                                emitQuadWithLiteralObjectNoNamespace(matcher);
                            }

                        }
                    }
                }

            }
        } catch (IOException ex) {
            throw new RDFHandlerException("failed processing likely nquad stream", ex);
        }

    }

    private void emitQuadWithIRIObject(Matcher matcher) {
        String subject = padIfNeeded(matcher, "subject");
        String verb = padIfNeeded(matcher, "verb");
        String object = padIfNeeded(matcher, "object");
        String namespace = padIfNeeded(matcher, "namespace");

        List<Quad> quads = RDFUtil.parseQuads(
                IOUtils.toInputStream("<" + subject + "> <" + verb + "> <" + object + "> <" + namespace + "> .", StandardCharsets.UTF_8)
        );
        quads.forEach(this::copyOnEmit);
    }

    private void emitQuadWithIRIObjectNoNamespace(Matcher matcher) {
        String subject = padIfNeeded(matcher, "subject");
        String verb = padIfNeeded(matcher, "verb");
        String object = padIfNeeded(matcher, "object");

        List<Quad> quads = RDFUtil.parseQuads(
                IOUtils.toInputStream("<" + subject + "> <" + verb + "> <" + object + "> <" + EmittingStreamOfAnyQuad.DEFAULT_PREFIX_X_PRESTON + "> .", StandardCharsets.UTF_8)
        );
        quads.forEach(this::copyOnEmit);
    }

    private String padIfNeeded(Matcher matcher, String termName) {
        // prefix relative IRIs to make RDF4J happy
        String term = matcher.group(termName);
        if (!StringUtils.contains(term, ":")) {
            term = DEFAULT_PREFIX_X_PRESTON + term;
        }
        return term;
    }

    private void emitQuadWithLiteralObject(Matcher matcher) {
        String subject = padIfNeeded(matcher, "subject");
        String verb = padIfNeeded(matcher, "verb");

        // no need to "fix" the object - it is a literal
        String object = matcher.group("object");

        String namespace = padIfNeeded(matcher, "namespace");


        List<Quad> quads = RDFUtil.parseQuads(
                IOUtils.toInputStream("<" + subject + "> <" + verb + "> " + object + " <" + namespace + "> .", StandardCharsets.UTF_8)
        );
        quads.forEach(this::copyOnEmit);
    }

    private void emitQuadWithLiteralObjectNoNamespace(Matcher matcher) {
        String subject = padIfNeeded(matcher, "subject");
        String verb = padIfNeeded(matcher, "verb");

        // no need to "fix" the object - it is a literal
        String object = matcher.group("object");

        List<Quad> quads = RDFUtil.parseQuads(
                IOUtils.toInputStream("<" + subject + "> <" + verb + "> " + object + " <" + EmittingStreamOfAnyQuad.DEFAULT_PREFIX_X_PRESTON + "> .", StandardCharsets.UTF_8)
        );
        quads.forEach(this::copyOnEmit);
    }


}
