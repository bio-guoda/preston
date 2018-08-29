package org.globalbioticinteractions.preston.process;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.Predicate;
import org.globalbioticinteractions.preston.store.StatementStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class ContentResolver extends RefStatementProcessor {

    private static Log LOG = LogFactory.getLog(CmdList.class);
    private final BlobStore store;
    private final StatementStore<URI> statementStore;

    public ContentResolver(BlobStore store, StatementStore<URI> statementStore, RefStatementListener... listeners) {
        super(listeners);
        this.store = store;
        this.statementStore = statementStore;

    }

    @Override
    public void on(RefStatement statement) {
        try {
            RefNode source = statement.getSource();
            RefNode relationType = statement.getRelationType();
            RefNode target = statement.getTarget();

            URI subject = getURI(source);
            URI predicate = getURI(relationType);
            URI object = getURI(target);

            statementStore.put(Triple.of(subject, predicate, object));

            if (object == null) {
                final URI key = statementStore.findKey(Pair.of(subject, Predicate.HAS_CONTENT_HASH));
                if (null != key) {

                    RefNode resolvedContentNode = new RefNode() {

                        @Override
                        public InputStream getContent() throws IOException {
                            return store.get(getContentHash());
                        }

                        @Override
                        public String getLabel() {
                            return getContentHash().toString();
                        }

                        @Override
                        public URI getContentHash() {
                            return key;
                        }

                        @Override
                        public boolean equivalentTo(RefNode node) {
                            URI id = getContentHash();
                            URI otherId = node == null ? null : node.getContentHash();
                            return id != null && id.equals(otherId);
                        }
                    };

                    emit(new RefStatement(source, RefNodeConstants.HAS_CONTENT, resolvedContentNode));

                }
            } else {
                emit(statement);
            }
        } catch (IOException e) {
            LOG.warn("failed to handle [" + statement.getLabel() + "]", e);
        }

    }

    private URI getURI(RefNode source) throws IOException {
        String s = source == null || source.getLabel() == null ? null : source.getLabel();
        return s == null ? null : URI.create(s);
    }


}
