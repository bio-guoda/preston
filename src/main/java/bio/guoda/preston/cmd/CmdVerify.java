package bio.guoda.preston.cmd;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "verifies integrity of the local biodiversity dataset graph")
public class CmdVerify extends Persisting implements Runnable {

    enum State {MISSING, INVALID_HASH, MATCHING_HASH}

    @Override
    public void run() {
        final BlobStore blobStore = new BlobStoreAppendOnly(getKeyValueStore());
        final StatementStore statementPersistence = new StatementStoreImpl(getKeyValueStore());


        Map<String, State> verifiedMap = new TreeMap<>();

        attemptReplay(blobStore, statementPersistence, new StatementListener() {
            @Override
            public void on(Triple statement) {
                IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
                    State state = State.MISSING;
                    CountingOutputStream counting = new CountingOutputStream(new NullOutputStream());
                    try (InputStream is = blobStore.get(iri)) {
                        if (is != null) {
                            IRI hashIRI = Hasher.calcSHA256(is, counting);
                            state = hashIRI.equals(iri) ? State.MATCHING_HASH : State.INVALID_HASH;
                        }
                    } catch (IOException e) {
                        //
                    } finally {
                        verifiedMap.put(iri.getIRIString(), state);
                        System.out.println(iri.getIRIString() + "\t" + (State.MATCHING_HASH == state ? "OK" : "FAIL") + "\t" + state + "\t" + counting.getByteCount());
                    }
                }
            }
        });
    }

}