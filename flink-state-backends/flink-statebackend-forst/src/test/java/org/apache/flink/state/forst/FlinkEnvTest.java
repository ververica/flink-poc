package org.apache.flink.state.forst;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.forst.fs.ForStFlinkFileSystem;
import org.apache.flink.testutils.oss.OSSTestCredentials;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Env;
import org.rocksdb.FlinkEnv;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class FlinkEnvTest {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvTest.class);

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

	@Test
	public void testLoadClass() {
		RocksDB.loadLibrary();
		try (FlinkEnv flinkEnv = new FlinkEnv("file:///test-dir")) {
			flinkEnv.testLoadClass("java/lang/Object");
			flinkEnv.testLoadClass("org/apache/flink/core/fs/local/LocalFileSystem");
			Assert.assertTrue(flinkEnv.testFileExits("/tmp"));
			Assert.assertFalse(flinkEnv.testFileExits("/tmp/test-xxx"));
		}

        final Configuration conf = new Configuration();
        initOSS(conf);
		FileSystem.initialize(conf);
		try (FlinkEnv flinkEnv = new FlinkEnv("oss://state-oss-test")) {
			Assert.assertTrue(flinkEnv.testFileExits("/flink-jobs"));
			Assert.assertFalse(flinkEnv.testFileExits("/test-xxx"));
		}
	}

    @Test
    public void testAccess() throws RocksDBException {
        final Configuration conf = new Configuration();
        initOSS(conf);
        FileSystem.initialize(conf, null);
        testAccess("hdfs://master-1-1.c-0849d7666eaf1f6c.cn-beijing.emr.aliyuncs.com:9000");
//        testAccess("oss://state-oss-test");
//        testAccess("file:///test-dir");
    }

    private void testAccess(String remoteDir) throws RocksDBException {
        ForStFlinkFileSystem.configureCacheBase(new Path("/tmp/local-cache"));
        try (final Env env = new FlinkEnv(remoteDir);
             final Options options = new Options();
             final Options newOptions = options.setCreateIfMissing(true).setUseFsync(false)
                     .setEnv(env)
                     .setDbLogDir(".")
                     .setLogger(new org.rocksdb.Logger(options) {
                         @Override
                         protected void log(InfoLogLevel infoLogLevel, String message) {
                             LOG.info("RocksDB [{}]: {}", infoLogLevel, message);
                         }
                     });
             final FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
             final RocksDB db = RocksDB.open(newOptions, "/tmp/test-remote-rocksdb")) {
            db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
            db.flush(flushOptions);
            assertThat(db.get("key1".getBytes(UTF_8))).isEqualTo("value1".getBytes(UTF_8));
        }
    }

    private void initOSS(Configuration conf) {
        OSSTestCredentials.assumeCredentialsAvailable();
        conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
        conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
        conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
    }
}
