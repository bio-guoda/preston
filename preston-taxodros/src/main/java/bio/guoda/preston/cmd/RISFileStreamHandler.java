package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.stream.ContentStreamHandler;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class RISFileStreamHandler implements ContentStreamHandler {
    private final Logger LOG = LoggerFactory.getLogger(RISFileStreamHandler.class);


    private final Persisting persisting;
    private final Dereferencer<InputStream> dereferencer;
    private final IRI provenanceAnchor;
    private final boolean ifAvailableReuseDOI;
    private final Dereferencer<IRI> doiForContent;
    private ContentStreamHandler contentStreamHandler;
    private final OutputStream outputStream;
    private final List<String> communities;

    public RISFileStreamHandler(ContentStreamHandler contentStreamHandler,
                                OutputStream os,
                                Persisting persisting,
                                Dereferencer<InputStream> dereferencer,
                                List<String> communities,
                                boolean ifAvailableReuseDOI,
                                Dereferencer<IRI> doiForContent) {
        this.contentStreamHandler = contentStreamHandler;
        this.outputStream = os;
        this.persisting = persisting;
        this.dereferencer = dereferencer;
        this.communities = communities;
        this.provenanceAnchor = AnchorUtil.findAnchorOrThrow(persisting);
        this.ifAvailableReuseDOI = ifAvailableReuseDOI;
        this.doiForContent = doiForContent;
    }

    @Override
    public boolean handle(IRI version, InputStream is) throws ContentStreamException {
        AtomicBoolean foundAtLeastOne = new AtomicBoolean(false);
        String iriString = version.getIRIString();
        try {
            RISUtil.parseRIS(is, new Consumer<ObjectNode>() {
                @Override
                public void accept(ObjectNode jsonNode) {
                    try {
                        ObjectNode zenodoObject = RISUtil.translateRISToZenodo(
                                jsonNode,
                                communities,
                                ifAvailableReuseDOI
                        );
                        ZenodoMetaUtil.appendIdentifier(zenodoObject, ZenodoMetaUtil.IS_PART_OF, provenanceAnchor.getIRIString());

                        Stream.of(HashType.md5, HashType.sha256)
                                .forEach(type ->
                                {
                                    try {
                                        String bhlPartPDFUrl = RISUtil.getBHLPartPDFUrl(zenodoObject);
                                        StreamHandlerUtil.appendContentId(
                                                zenodoObject,
                                                bhlPartPDFUrl,
                                                type,
                                                dereferencer,
                                                persisting);
                                    } catch (ContentStreamException e) {
                                        LOG.warn("failed to find [" + type.name() + "] content id related to metadata for [" + jsonNode.toPrettyString() + "]", e);
                                        // no hash, no content
                                    }
                                });

                        IRI doi = doiForContent.get(version);
                        if (doi != null) {
                            ZenodoMetaUtil.appendIdentifier(
                                    zenodoObject,
                                    ZenodoMetaUtil.IS_ALTERNATE_IDENTIFIER,
                                    doi.getIRIString()
                            );
                            if (ifAvailableReuseDOI) {
                                if (!zenodoObject.has("doi")) {
                                    zenodoObject.put("doi", doi.getIRIString());
                                }
                            }
                        }
                        StreamHandlerUtil.writeRecord(foundAtLeastOne, zenodoObject, outputStream);
                    } catch (IOException e) {
                        LOG.warn("failed to process [" + jsonNode.toPrettyString() + "]", e);
                    }
                }
            }, iriString, persisting);

        } catch (IOException e) {
            // opportunistic parsing, so ignore exceptions
        }
        return foundAtLeastOne.get();
    }


    @Override
    public boolean shouldKeepProcessing() {
        return contentStreamHandler.shouldKeepProcessing();
    }


}
