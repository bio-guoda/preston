package org.globalbioticinteractions.preston.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.Predicate;
import org.globalbioticinteractions.preston.store.RelationStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;

public class BlobStoreWriter extends RefNodeProcessor {

    private static Log LOG = LogFactory.getLog(CmdList.class);
    private final BlobStore store;
    private final RelationStore<URI> relationStore;

    public BlobStoreWriter(BlobStore store, RelationStore<URI> relationStore, RefNodeListener... listeners) {
        super(listeners);
        this.store = store;
        this.relationStore = relationStore;

    }

    @Override
    public void on(RefNodeRelation relation) {
        try {
            RefNode source = relation.getSource();
            RefNode relationType = relation.getRelationType();
            RefNode target = relation.getTarget();

            URI subject = getURI(source);
            URI predicate = getURI(relationType);
            URI object = getURI(target);
            relationStore.put(Triple.of(subject, predicate, object));
            if (object == null) {
                final URI key = relationStore.findKey(Pair.of(subject, Predicate.HAS_CONTENT_HASH));
                if (null != key) {

                    RefNode resolvedContentNode = new RefNode() {

                        @Override
                        public InputStream getData() throws IOException {
                            return store.get(getId());
                        }

                        @Override
                        public String getLabel() {
                            return getId().toString();
                        }

                        @Override
                        public URI getId() {
                            return key;
                        }

                        @Override
                        public boolean equivalentTo(RefNode node) {
                            URI id = getId();
                            URI otherId = node == null ? null : node.getId();
                            return id != null && id.equals(otherId);
                        }
                    };

                    emit(new RefNodeRelation(source, RefNodeConstants.HAS_CONTENT, resolvedContentNode));

                }
            } else {
                emit(relation);
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + relation.getLabel() + "]", e);
        }

    }

    private URI getURI(RefNode source) throws IOException {
        String s = source == null || source.getLabel() == null ? null : source.getLabel();
        return s == null ? null : URI.create(s);
    }


}
