package com.netflix.conductor.dao.mysql;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.DecryptionFailureException;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.amazonaws.services.secretsmanager.model.InternalServiceErrorException;
import com.amazonaws.services.secretsmanager.model.InvalidParameterException;
import com.amazonaws.services.secretsmanager.model.InvalidRequestException;
import com.amazonaws.services.secretsmanager.model.ResourceNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.config.Configuration;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Base64.getDecoder;


@SuppressWarnings("Duplicates")
public class MySQLDAOTestUtil {
    private static final Logger logger = LoggerFactory.getLogger(MySQLDAOTestUtil.class);
    private final HikariDataSource dataSource;
    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();

    MySQLDAOTestUtil(String dbName) throws Exception {
        testConfiguration.setProperty("jdbc.url", "jdbc:mysql://localhost:33307/" + dbName + "?useSSL=false&useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC");
        testConfiguration.setProperty("jdbc.username", "root");
        testConfiguration.setProperty("jdbc.password", "");
        // Ensure the DB starts
        EmbeddedDatabase.INSTANCE.getDB().createDB(dbName);

        this.dataSource = getDataSource(testConfiguration);
    }

    private HikariDataSource getDataSource(Configuration config) {

        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getProperty("jdbc.url", "jdbc:mysql://localhost:33307/conductor"));
        dataSource.setUsername(config.getProperty("jdbc.username", "conductor"));
        dataSource.setPassword(config.getProperty("jdbc.password", "password"));
        dataSource.setAutoCommit(false);

        // Prevent DB from getting exhausted during rapid testing
        dataSource.setMaximumPoolSize(8);

        flywayMigrate(dataSource);

        return dataSource;
    }

    private void flywayMigrate(DataSource dataSource) {

        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        flyway.setPlaceholderReplacement(false);
        flyway.migrate();
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    public TestConfiguration getTestConfiguration() {
        return testConfiguration;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public void resetAllData() {
        logger.info("Resetting data for test");
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet rs = connection.prepareStatement("SHOW TABLES").executeQuery();
                 PreparedStatement keysOn = connection.prepareStatement("SET FOREIGN_KEY_CHECKS=1")) {
                try (PreparedStatement keysOff = connection.prepareStatement("SET FOREIGN_KEY_CHECKS=0")) {
                    keysOff.execute();
                    while (rs.next()) {
                        String table = rs.getString(1);
                        try (PreparedStatement ps = connection.prepareStatement("TRUNCATE TABLE " + table)) {
                            ps.execute();
                        }
                    }
                } finally {
                    keysOn.execute();
                }
            }
        } catch (SQLException ex) {
            logger.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    public static void getSecret() {

        String secretName = "arn:aws:secretsmanager:ap-southeast-2:053457794187:secret:DBInstanceRotationSecret-DB92FAQwTHjW-qLOFQI";
        String region = "ap-southeast-2";

        // Create a Secrets Manager client
        AWSSecretsManager client = AWSSecretsManagerClientBuilder.standard()
                .withRegion(region)
                .build();

        // In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        // See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        // We rethrow the exception by default.

        String secret, decodedBinarySecret;
        GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                .withSecretId(secretName);
        GetSecretValueResult getSecretValueResult = null;

        try {
            getSecretValueResult = client.getSecretValue(getSecretValueRequest);
        } catch (DecryptionFailureException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InternalServiceErrorException e) {
            // An error occurred on the server side.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidParameterException e) {
            // You provided an invalid value for a parameter.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (InvalidRequestException e) {
            // You provided a parameter value that is not valid for the current state of the resource.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        } catch (ResourceNotFoundException e) {
            // We can't find the resource that you asked for.
            // Deal with the exception here, and/or rethrow at your discretion.
            throw e;
        }

        // Decrypts secret using the associated KMS CMK.
        // Depending on whether the secret is a string or binary, one of these fields will be populated.
        if (getSecretValueResult.getSecretString() != null) {
            secret = getSecretValueResult.getSecretString();
            logger.warn("===================AWS SECRET=======================");
            logger.warn(secret);
            logger.warn("===================AWS SECRET=======================");
        } else {
            decodedBinarySecret = new String(getDecoder()
                    .decode(getSecretValueResult.getSecretBinary())
                    .array()
            );
            logger.warn("===================AWS DECODED SECRET=======================");
            logger.warn(decodedBinarySecret);
            logger.warn("===================AWS DECODED SECRET=======================");

        }
        // Your code goes here.
    }
}
