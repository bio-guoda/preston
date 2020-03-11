package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class CmdUpdateTest {

    @Test
    public void doUpdate() {
        new CmdUpdate().run(
                new BlobStoreNull(),
                new StatementStoreNull());


    }

    private static class BlobStoreNull implements BlobStore {
        @Override
        public IRI put(InputStream is) throws IOException {
            return null;
        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }

    private static class StatementStoreNull implements StatementStore {
        @Override
        public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {

        }

        @Override
        public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
            return null;
        }
    }
}