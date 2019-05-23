package bio.guoda.preston.cmd;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyToPath;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "verifies completeness and integrity of the local biodiversity dataset graph")
public class CmdVerify extends Persisting implements Runnable {

    public static final List<State> OK_STATES = Arrays.asList(
            State.CONTENT_PRESENT_VALID_HASH,
            State.CONTENT_PRESENT_HASH_NOT_VERIFIED);

    enum State {
        MISSING,
        CONTENT_PRESENT_INVALID_HASH,
        CONTENT_PRESENT_VALID_HASH,
        CONTENT_PRESENT_HASH_NOT_VERIFIED
    }

    ;

    @Parameter(names = {"--skip-hash-verification"}, description = "do not verify hash, just check availability")
    private Boolean skipHashVerification = false;


    @Override
    public void run() {
        final BlobStore blobStore = new BlobStoreAppendOnly(getKeyValueStoreLocal());
        final StatementStore statementPersistence = new StatementStoreImpl(getKeyValueStoreLocal());

        List<KeyToPath> keyToPathList = new ArrayList<KeyToPath>() {{
            if (hasRemote()) {
                add(new KeyTo1LevelPath(getRemoteURI()));
            }
            add(new KeyTo3LevelPath(getDefaultDataDir().toURI()));
        }};

        Map<String, State> verifiedMap = new TreeMap<>();

        attemptReplay(blobStore, statementPersistence, new StatementListener() {
            @Override
            public void on(Triple statement) {
                final IRI iri = VersionUtil.mostRecentVersionForStatement(statement);
                if (iri != null && !verifiedMap.containsKey(iri.getIRIString())) {
                    State state = State.MISSING;
                    long fileSize = 0;
                    try (InputStream is = blobStore.get(iri)) {
                        if (is != null) {
                            if (skipHashVerification) {
                                // try a shortcut via filesystem
                                URI uri = getKeyToPathLocal().toPath(iri.getIRIString());
                                fileSize = new File(uri).length();
                                if (fileSize == 0) {
                                    try (CountingOutputStream counting = new CountingOutputStream(new NullOutputStream())) {
                                        IOUtils.copy(is, counting);
                                        fileSize = counting.getByteCount();
                                    }
                                }
                                state = State.CONTENT_PRESENT_HASH_NOT_VERIFIED;
                            } else {
                                try (CountingOutputStream counting = new CountingOutputStream(new NullOutputStream())) {
                                    IRI hashIRI = Hasher.calcSHA256(is, counting);
                                    state = hashIRI.equals(iri) ? State.CONTENT_PRESENT_VALID_HASH : State.CONTENT_PRESENT_INVALID_HASH;
                                    fileSize = counting.getByteCount();
                                }
                            }
                        }
                    } catch (IOException e) {
                        //
                    } finally {
                        verifiedMap.put(iri.getIRIString(), state);
                        String iriString = iri.getIRIString();
                        for (KeyToPath keyToPath : keyToPathList) {
                            System.out.println(iriString + "\t" +
                                    keyToPath.toPath(iriString) + "\t" +
                                    (OK_STATES.contains(state) ? "OK" : "FAIL") + "\t" +
                                    state + "\t" +
                                    fileSize);
                            System.out.flush();
                        }
                    }
                }
            }
        });
    }

}
