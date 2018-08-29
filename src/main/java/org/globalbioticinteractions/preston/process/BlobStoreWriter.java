package org.globalbioticinteractions.preston.process;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.cmd.CmdList;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.Predicate;
import org.globalbioticinteractions.preston.store.RelationStore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
                final String key = relationStore.findKey(Pair.of(subject, Predicate.HAS_CONTENT_HASH));
                if (StringUtils.isNotBlank(key)) {

                    RefNode resolvedContentNode = new RefNode() {

                        @Override
                        public InputStream getData() throws IOException {
                            return store.get(getId());
                        }

                        @Override
                        public String getLabel() {
                            return "preston:" + getId();
                        }

                        @Override
                        public String getId() {
                            return key;
                        }

                        @Override
                        public boolean equivalentTo(RefNode node) {
                            return StringUtils.equals(node.getId(), getId());
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

    public static File getDataFile(String id, File dataDir) {
        return new File(getDatasetDir(id, dataDir), "data");
    }

    public static File getDatasetDir(String id, File dataDir) {
        return new File(dataDir, toPath(id));
    }

    public static String toPath(String id) {
        if (StringUtils.length(id) < 8) {
            throw new IllegalArgumentException("expected id [" + id + "] of at least 8 characters");
        }
        String u0 = id.substring(0, 2);
        String u1 = id.substring(2, 4);
        String u2 = id.substring(4, 6);
        return StringUtils.join(Arrays.asList(u0, u1, u2, id), "/");
    }


}
