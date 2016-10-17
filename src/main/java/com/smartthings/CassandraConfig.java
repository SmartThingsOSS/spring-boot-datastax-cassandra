package com.samsung.iotcloud;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.List;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import smartthings.migration.MigrationParameters;
import smartthings.migration.MigrationRunner;

//TODO Move this to a shared library
@Slf4j
@Data
@Configuration
@ConfigurationProperties(prefix = "cassandra")
public class CassandraConfig {

	/* SSL */
	@Data
	public static class JKSConfig {
		private String path;
		private String password;

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}

		public String getPassword() {
			return password;
		}

		public void setPassword(String password) {
			this.password = password;
		}
	}

	private static final String[] cipherSuites = new String[]{"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"};
	private JKSConfig truststore;
	private JKSConfig keystore;

	/* variables of property */
	private String keyspace;
	private String migrationPath;
	private Boolean autoMigrate;
	private String ddlKeyspaceFile;
	private String user;
	private String password;
	private List<String> endpoints;
	private String migrationFile;

	private void migration(Session session) {

		log.info("Auto Migrating Cassandra");
		MigrationRunner migrationRunner = new MigrationRunner();

		MigrationParameters parameters = new MigrationParameters.Builder()
			.setKeyspace(keyspace)
			.setMigrationsLogFile(migrationFile)
			.setSession(session).build();

		migrationRunner.run(parameters);
	}

	@Bean
	public Session session() throws Exception {

		DCAwareRoundRobinPolicy dcAwareRoundRobinPolicy = DCAwareRoundRobinPolicy.builder().withUsedHostsPerRemoteDc(1).build();

		Cluster.Builder builder = Cluster.builder();

		for (String address : endpoints) {
			if (address.contains(":")) {
				String[] parts = address.split(":");
				builder.addContactPointsWithPorts(new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
			} else {
				builder.addContactPoint(address);
			}

		}
		builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwareRoundRobinPolicy));

		if (keystore != null && truststore != null) {
			SSLContext sslContext = getSSLContext(
				truststore.getPath(),
				truststore.getPassword(),
				keystore.getPath(),
				keystore.getPassword());
			JdkSSLOptions options = JdkSSLOptions.builder().withSSLContext(sslContext).build();
			builder.withSSL(options);
		}

		if (user != null && password != null) {
			builder.withCredentials(user, password);
		}

		Session session = builder.build().connect(keyspace);

		if (autoMigrate) {
			migration(session);
		}

		return session;
	}

	public SSLContext getSSLContext(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) throws Exception {

			KeyStore truststore = loadKeystore(truststorePath, truststorePassword);
			TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			trustManagerFactory.init(truststore);

			KeyStore keystore = loadKeystore(keystorePath, keystorePassword);
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			keyManagerFactory.init(keystore, keystorePassword.toCharArray());

			SSLContext context = SSLContext.getInstance("SSL");
			context.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
			return context;
	}

	private KeyStore loadKeystore(String path, String password) throws Exception {
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(path);
			KeyStore keystore = KeyStore.getInstance("JKS");
			keystore.load(fileInputStream, password.toCharArray());
			return keystore;
		} finally {
			if (fileInputStream != null) {
				fileInputStream.close();
			}
		}
	}

}
