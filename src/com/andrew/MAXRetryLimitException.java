package com.andrew;

/**
 * Created by Andrew on 30/10/2016.
 */
public class MAXRetryLimitException extends Exception {
    public int MaxRetryTime;

    public MAXRetryLimitException(int maxRetryTime) {
        MaxRetryTime = maxRetryTime;
    }
}
