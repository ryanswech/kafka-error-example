package com.example.kafka.error.services.kafka.data;

public class CrashableRequest
{
    private int requestID;
    private boolean shouldCrash;

    /**
     * For deserialization
     */
    private CrashableRequest()
    {}

    public CrashableRequest(int requestID, boolean shouldCrash)
    {
        this.requestID = requestID;
        this.shouldCrash = shouldCrash;
    }

    public int getRequestID()
    {
        return this.requestID;
    }

    public boolean isShouldCrash()
    {
        return this.shouldCrash;
    }

    public boolean getShouldCrash()
    {
        return this.shouldCrash;
    }
}
