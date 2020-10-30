package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.TextReader;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static java.lang.System.exit;
import static org.apache.commons.io.IOUtils.EOF;

@Parameters(separators = "= ", commandDescription = "get biodiversity data")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "data content-hash uri (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> contentUris = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        try {
            if (contentUris.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    handleContentIri(blobStore, StringUtils.trim(line));
                }
            } else {
                for (String s : contentUris) {
                    handleContentIri(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }
    }

    protected void handleContentIri(BlobStoreReadOnly blobStore, String queryString) throws IOException {
        ContentQuery query = new ContentQuery(queryString);
        IRI contentHash = query.getContentIri();

        try {
            InputStream input = blobStore.get(contentHash);
            if (input == null) {
                System.err.print("not found: [" + contentHash.getIRIString() + "]\n");
                exit(1);
            }
            processQuery(query, input);

        } catch (IOException | URISyntaxException e) {
            throw new IOException("problem retrieving [" + contentHash.getIRIString() + "]", e);
        }
    }

    private void processQuery(ContentQuery query, InputStream is) throws IOException, URISyntaxException {
        query.dereference(is, System.out);
    }

    protected void copyIfNoError(InputStream proxyIs) throws IOException {
        byte[] buffer = new byte[4096];
        int n;
        while (this.shouldKeepProcessing() && !System.out.checkError() && EOF != (n = proxyIs.read(buffer))) {
            System.out.write(buffer, 0, n);
        }
    }

    public void setContentUris(List<String> contentUris) {
        this.contentUris = contentUris;
    }

    private class ContentQuery extends TextReader {

        String targetIriString;
        long byteStart;
        long byteEnd;
        private IRI contentIri;
        private PrintStream printStream;

        public ContentQuery(String targetIriString) {
            this.targetIriString = targetIriString;

            final Pattern contentHashPattern = Pattern.compile("hash://sha256/[a-fA-F0-9]{64}");
            Matcher matchHash = contentHashPattern.matcher(targetIriString);
            if (matchHash.find()) {
                contentIri = toIRI(matchHash.group());
            }
        }

        public IRI getContentIri() {
            return contentIri;
        }

        public void dereference(InputStream is, PrintStream out) throws IOException, URISyntaxException {
            printStream = out;
            attemptToParse(contentIri, is);
        }

        @Override
        protected boolean shouldReadArchiveEntry(IRI entryIri) {
            return (targetIriString.contains(entryIri.getIRIString()));
        }

        @Override
        protected void parseAsText(IRI version, InputStream in, Charset charset) throws IOException {
            // do not support open-ended cuts, e.g. "b5-" or "b-5"
            Matcher matchByteLocation = Pattern.compile(String.format("^cut:%s!/b(?<first>[0-9]+)-(?<last>[0-9]+)", version.getIRIString())).matcher(targetIriString);
            if (matchByteLocation.find()) {
                long firstByteIndex = Long.parseLong(matchByteLocation.group("first")) - 1;
                long lastByteIndex = Long.parseLong(matchByteLocation.group("last"));

                IOUtils.copyLarge(in, printStream, firstByteIndex, (lastByteIndex - firstByteIndex));
            }
            else if (targetIriString.equals(version.getIRIString())) {
                IOUtils.copy(in, printStream);
            }
            else {
                throw new IOException();
            }
        }
    }

}
