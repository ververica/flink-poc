package org.apache.flink.state.remote.rocksdb;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.testutils.oss.OSSTestCredentials;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.FlinkEnv;
import org.rocksdb.RocksDB;

public class FlinkEnvTest {

	@Test
	public void testLoadClass() {
		RocksDB.loadLibrary();
		try (FlinkEnv flinkEnv = new FlinkEnv("file:///test-dir")) {
			flinkEnv.testLoadClass("java/lang/Object");
			flinkEnv.testLoadClass("org/apache/flink/core/fs/local/LocalFileSystem");
			Assert.assertTrue(flinkEnv.testFileExits("/tmp"));
			Assert.assertFalse(flinkEnv.testFileExits("/tmp/test-xxx"));
		}

		OSSTestCredentials.assumeCredentialsAvailable();
		final Configuration conf = new Configuration();
		conf.setString("fs.oss.endpoint", OSSTestCredentials.getOSSEndpoint());
		conf.setString("fs.oss.accessKeyId", OSSTestCredentials.getOSSAccessKey());
		conf.setString("fs.oss.accessKeySecret", OSSTestCredentials.getOSSSecretKey());
		FileSystem.initialize(conf);
		try (FlinkEnv flinkEnv = new FlinkEnv("oss://state-oss-test")) {
			Assert.assertTrue(flinkEnv.testFileExits("/flink-jobs"));
			Assert.assertFalse(flinkEnv.testFileExits("/test-xxx"));
		}
	}
}
