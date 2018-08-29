package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.FilePersistence;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static java.lang.System.exit;

@Parameters(separators = "= ", commandDescription = "Prints Biodiversity Graph Node")
public class CmdGet implements Runnable {

    @Parameter(description = "node id (e.g., [hash://sha256/8ed3110302c38077eace003a67bbfebefc0e2e2c9e67c8703ca49355514bdec9] )",
            required = true,
            validateWith = URIValidator.class)
    private String contentHashString  = null;


    @Override
    public void run() {
        FilePersistence persistence = new FilePersistence();
        AppendOnlyBlobStore blobStore = new AppendOnlyBlobStore(persistence);

        try {
            InputStream input = blobStore.get(URI.create(contentHashString));
            if (input == null) {
                System.err.println("no found: [" + contentHashString + "]");
                exit(1);
            }
            IOUtils.copy(input, System.out);
            exit(0);
        } catch (IOException e) {
            System.err.println("problem retrieving [" + contentHashString + "]");
            e.printStackTrace(System.err);
            exit(1);
        }

    }

}
