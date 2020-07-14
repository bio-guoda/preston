package bio.guoda.preston.cmd;

import bio.guoda.preston.process.SimilarContentFinder;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TikaHashingActivity;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameters;

import java.util.stream.Stream;

@Parameters(separators = "= ", commandDescription = "Describes similarity between contents identified by hash://sha256 IRIs according to their hash://tika-tlsh IRIs")
public class CmdSimilar extends CmdProcess {

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStore blobStore, StatementsListener queueAsListener) {
        return Stream.of(
                new SimilarContentFinder(blobStore, queueAsListener)
        );
    }

    @Override
    String getActivityDescription() {
        return "An event that generates similarity values between encountered contents (e.g., hash://sha256/... identifiers) that have been associated with a similarity hash (e.g., hash://tika-tlsh/...). The process currently uses similarity hashes (e.g., Trend Micro Locality Sensitive Hash, https://github.com/trendmicro/tlsh/blob/master/TLSH_CTC_final.pdf) calculated on content text extracted using https://tika.apache.org.";
    }

}
