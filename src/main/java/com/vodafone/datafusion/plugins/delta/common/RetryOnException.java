package com.vodafone.datafusion.plugins.delta.common;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vodafone.datafusion.plugins.delta.constants.Constants.*;

/**
 * Encapsulates retry-on-exception operations
 */
public class RetryOnException {
    private static final Logger LOG = LoggerFactory.getLogger(RetryOnException.class);

    public static final String DEFAULT_RETRIES = ZERO_STRING;
    public static final String DEFAULT_TIME_TO_WAIT_MS = ZERO_STRING;

    private int numRetries;
    private long timeToWaitMS;

    public RetryOnException(String _numRetries,
                            String _timeToWait) {
        numRetries = Strings.isNullOrEmpty(_numRetries)?0:Integer.parseInt(_numRetries);
        timeToWaitMS = Strings.isNullOrEmpty(_timeToWait)?0:Long.parseLong(_timeToWait)*1000;
    }

    public RetryOnException() {
        this(DEFAULT_RETRIES, DEFAULT_TIME_TO_WAIT_MS);
    }

    /**
     * Returns true if a retry can be attempted.
     * @return  True if retries attempts remain; else false
     */
    public boolean shouldRetry() {
        return (numRetries >= 0);
    }

    /**
     * Waits for timeToWaitMS. Ignores any interrupted exception
     */
    public void waitUntilNextTry() {
        try {
            Thread.sleep(timeToWaitMS);
        } catch (InterruptedException iex) {
            iex.printStackTrace();
        }
    }

    /**
     * Call when an exception has occurred in the block. If the
     * retry limit is exceeded, throws an exception.
     * Else waits for the specified time.
     * @throws Exception
     */
    public void exceptionOccurred(String retryFile) throws Exception {
        numRetries--;
        String fileName = retryFile.startsWith(SLASH) ? retryFile.substring(1) : retryFile;
        if(!shouldRetry()) {
            throw new Exception("[SFTP Delta] Retry limit exceeded for file: "  + fileName);
        }
        LOG.info("[SFTP Delta] Retrying connection for file: "  + fileName);
        waitUntilNextTry();
    }
}
