package bio.guoda.preston;

import org.hamcrest.core.Is;
import org.junit.Test;

import static org.junit.Assert.*;

public class RateLimitedRetryStrategyTest {

    @Test
    public void exponentialDelayFirstRetry() {
        assertThat(
                RateLimitedRetryStrategy.calculateExponentialDelayFor(0),
                Is.is(30)
        );
    }

    @Test
    public void exponentialDelaySecondRetry() {
        assertThat(
                RateLimitedRetryStrategy.calculateExponentialDelayFor(1),
                Is.is(300)
        );
    }

    @Test
    public void exponentialDelayThirdRetry() {
        assertThat(
                RateLimitedRetryStrategy.calculateExponentialDelayFor(2),
                Is.is(3000)
        );
    }

}