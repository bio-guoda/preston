package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.lang3.StringUtils;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static java.lang.System.exit;
import static java.lang.System.in;
import static org.apache.commons.io.IOUtils.EOF;

@Parameters(separators = "= ", commandDescription = "get biodiversity node(s)")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "node id (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> hashes = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore());

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

    public void handleHash(BlobStoreAppendOnly blobStore, String hash) throws IOException {
        try {
            InputStream input = blobStore.get(RefNodeFactory.toIRI(hash));
            if (input == null) {
                System.err.println("not found: [" + hash + "]");
                exit(1);
            }
            copyIfNoError(input, System.out);

        } catch (IOException e) {
            throw new IOException("problem retrieving [" + hash + "]", e);
        }
    }

    public static void copyIfNoError(InputStream proxyIs, PrintStream out) throws IOException {
        byte[] buffer = new byte[4096];
        int n;
        while (!out.checkError() && EOF != (n = proxyIs.read(buffer))) {
            System.out.write(buffer, 0, n);
        }
    }

}
