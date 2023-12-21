package org.apache.flink.streaming.examples.wordcount2;

public class LocalWordCount {
    public static void main(String[] args) throws Exception {
        String[] parameters = new String[] {
                "-" + JobConfig.STATE_BACKEND.key(),
                "ROCKSDB",
                "-" + JobConfig.CHECKPOINT_PATH.key(),
                "file:///tmp",
                "-" + JobConfig.SHARING_GROUP.key(),
                "true"
        };
        WordCount.main(parameters);
    }
}
