package org.apache.flink;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Env;
import org.rocksdb.HdfsEnv;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class RocksDBHdfsTest {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBHdfsTest.class);

    @TempDir private Path dbFolder;

    @Test
    public void testHDFS() throws Exception {
        try (final Env env = new HdfsEnv("hdfs://localhost:9000");
             final Options options = new Options();
             final Options newOptions = options.setCreateIfMissing(true)
                     .setEnv(env)
                     .setDbLogDir(".")
                     .setLogger(new org.rocksdb.Logger(options) {
                         @Override
                         protected void log(InfoLogLevel infoLogLevel, String message) {
                             LOG.info("RocksDB [{}]: {}", infoLogLevel, message);
                         }
                     });
             final RocksDB db = RocksDB.open(newOptions, dbFolder.toString())) {
            db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
            assertThat(db.get("key1".getBytes(UTF_8))).isEqualTo("value1".getBytes(UTF_8));
        }
    }
}
