package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TikaHashingActivity;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameters;

import java.util.stream.Stream;

@Parameters(separators = "= ", commandDescription = "Generates Tika TLSH associated with any encountered hash://sha256 IRIs")
public class CmdTika extends CmdProcess {

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStore blobStore, StatementsListener queueAsListener) {
        return Stream.of(
                new TikaHashingActivity(blobStore, queueAsListener)
        );
    }

    @Override
    String getActivityDescription() {
        return "An event that calculates, if possible, a similarity hash (e.g., hash://tika-tlsh/...) of any encountered content (e.g., hash://sha256/... identifiers). The calculation currently involves generating a similarity hash (e.g., Trend Micro Locality Sensitive Hash, https://github.com/trendmicro/tlsh/blob/master/TLSH_CTC_final.pdf) after extracting the content text using https://tika.apache.org.";
    }


}
