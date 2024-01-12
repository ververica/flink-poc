package org.apache.flink.runtime.state.async;

public class BatchCacheStateConfig {

    private final boolean enableCache;

    public BatchCacheStateConfig(boolean enableCache) {
        this.enableCache = enableCache;
    }

    public boolean isEnableCacheBatchData() {
        return enableCache;
    }
}
