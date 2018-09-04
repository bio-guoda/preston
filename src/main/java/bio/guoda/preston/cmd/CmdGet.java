package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.AppendOnlyBlobStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;

@Parameters(separators = "= ", commandDescription = "get biodiversity node(s)")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "node id (e.g., [hash://sha256/8ed311...]). Waits for stdin if none are specified.",
            validateWith = URIValidator.class)
    private List<String> hashes = new ArrayList<>();

    @Override
    public void run() {
        AppendOnlyBlobStore blobStore = new AppendOnlyBlobStore(getBlobPersistence());

        try {
            if (hashes.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    handleHash(blobStore, StringUtils.trim(line));
                }
            } else {
                for (String s : hashes) {
                    handleHash(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }

        exit(0);
    }

    public void handleHash(AppendOnlyBlobStore blobStore, String hash) throws IOException {
        try {
            InputStream input = blobStore.get(RefNodeFactory.toIRI(hash));
            if (input == null) {
                System.err.println("not found: [" + hash + "]");
                exit(1);
            }
            IOUtils.copy(input, System.out);

        } catch (IOException e) {
            throw new IOException("problem retrieving [" + hash + "]", e);
        }
    }

}
