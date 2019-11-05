package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

public class DerefProgressLogger implements DerefProgressListener {
    private final AtomicLong lastRead = new AtomicLong(0L);
    private final PrintStream out;
    private AtomicLong updateStepBytes = new AtomicLong(10 * 4096L);
    private StopWatch stopWatch = new StopWatch();

    public DerefProgressLogger() {
        this(System.err);
    }

    public DerefProgressLogger(PrintStream out) {
        this.out = out;
    }

    @Override
    public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {
        if (DerefState.START.equals(derefState)) {
            reset();
            stopWatch.start();
        } else if (DerefState.DONE.equals(derefState)) {
            checkState();
            stopWatch.split();
            logProgress(dataURI, derefState, read, total);
            stopWatch.stop();
        } else if (DerefState.BUSY.equals(derefState)) {
            checkState();
            stopWatch.split();
            if (read - lastRead.get() > getUpdateStepBytes()) {
                logProgress(dataURI, derefState, read, total);
            }
        }
    }

    private void checkState() {
        if (!stopWatch.isStarted()) {
            reset();
            stopWatch.start();
        }
    }

    private void reset() {
        stopWatch.reset();
        lastRead.set(0);
    }

    public void logProgress(IRI dataURI, DerefState derefState, long read, long total) {
        StringBuilder builder = new StringBuilder();
        builder.append("\r[");
        builder.append(StringUtils.abbreviateMiddle(dataURI.getIRIString(), "...", 50));
        builder.append("] ");
        if (total > 0) {
            builder.append(String.format("%3.1f", 100.0 * read / total));
            builder.append("% of ");
            appendSize(total, builder);
        } else {
            appendSize(read, builder);
        }
        double elapsedTime = stopWatch.getSplitTime() / 1000.0;
        String rateInMBPerSecond = elapsedTime > 0 ? String.format("%.2f", read / (1024 * 1024.0 * elapsedTime)) : "?";
        String etaInMinutes = elapsedTime > 1 ? String.format("+%.0f minutes", total * (elapsedTime / (read * 60.0))) : "< 1 minute";
        builder.append(rateInMBPerSecond);
        builder.append(" MB/s");
        if (DerefState.DONE.equals(derefState)) {
            builder.append(" completed in ");
            builder.append(elapsedTime / 60.0 < 1.0 ? "< 1 minute" : String.format("%.0f minute(s)", elapsedTime / 60.0));
        } else if (total > 0) {
            builder.append(" ETA: ");
            builder.append(etaInMinutes);
        }

        if (DerefState.DONE.equals(derefState)) {
            builder.append("\n");
        }
        out.print(builder.toString());
        lastRead.set(read);
    }

    public void appendSize(long read, StringBuilder builder) {
        if (read < 1024) {
            builder.append(read);
            builder.append(" bytes at ");
        } else if (read < 1024 * 1024) {
            builder.append(read / 1024);
            builder.append(" kB at ");
        } else if (read > 1024 * 1024) {
            builder.append(read / (1024 * 1024));
            builder.append(" MB at ");
        }
    }

    public long getUpdateStepBytes() {
        return this.updateStepBytes.get();
    }

    public void setUpdateStepBytes(long updateStepBytes) {
        this.updateStepBytes.set(updateStepBytes);
    }
}
