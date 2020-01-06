/*
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.jetty.server;

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
import com.google.inject.servlet.GuiceFilter;
import com.netflix.conductor.bootstrap.Main;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.service.Lifecycle;
import com.sun.jersey.api.client.Client;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

import static java.lang.Boolean.getBoolean;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Base64.getDecoder;
import static org.eclipse.jetty.util.log.Log.getLog;

/**
 * @author Viren
 */
public class JettyServer implements Lifecycle {

    private static Logger logger = LoggerFactory.getLogger(JettyServer.class);

    private final int port;
    private final boolean join;

    private Server server;


    public JettyServer(int port, boolean join) {
        this.port = port;
        this.join = join;
    }


    @Override
    public synchronized void start() throws Exception {

        if (server != null) {
            throw new IllegalStateException("Server is already running");
        }

        this.server = new Server(port);

        ServletContextHandler context = new ServletContextHandler();
        context.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
        context.setWelcomeFiles(new String[]{"index.html"});

        server.setHandler(context);
        if (getBoolean("enableJMX")) {
            System.out.println("configure MBean container...");
            configureMBeanContainer(server);
        }
        server.start();
        System.out.println("Started server on http://localhost:" + port + "/");
        try {
            getSecret();
            boolean create = getBoolean("loadSample");
            if (create) {
                if (!kitchenSinkExists(port)) {
                    System.out.println("Creating kitchensink workflow");
                    createKitchenSink(port);
                } else {
                    logger.warn("kitchenSink already exists!!!");
                }
            }
        } catch (Exception e) {
            logger.error("Error loading sample!", e);
        }

        if (join) {
            server.join();
        }

    }

    public synchronized void stop() throws Exception {
        if (server == null) {
            throw new IllegalStateException("Server is not running.  call #start() method to start the server");
        }
        server.stop();
        server = null;
    }


    private static void createKitchenSink(int port) throws Exception {
        Client client = Client.create();
        ObjectMapper objectMapper = new ObjectMapper();


        List<TaskDef> taskDefs = new LinkedList<>();
        for (int i = 0; i < 40; i++) {
            taskDefs.add(new TaskDef("task_" + i, "task_" + i, 1, 0));
        }
        taskDefs.add(new TaskDef("search_elasticsearch", "search_elasticsearch", 1, 0));

        client.resource("http://localhost:" + port + "/api/metadata/taskdefs").type(MediaType.APPLICATION_JSON).post(objectMapper.writeValueAsString(taskDefs));

        /*
         * Kitchensink example (stored workflow with stored tasks)
         */
        InputStream stream = Main.class.getResourceAsStream("/kitchensink.json");
        client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

        stream = Main.class.getResourceAsStream("/sub_flow_1.json");
        client.resource("http://localhost:" + port + "/api/metadata/workflow").type(MediaType.APPLICATION_JSON).post(stream);

        logger.info("Kitchen sink workflow definition is created!");
    }

    private static boolean kitchenSinkExists(int port) {
        WorkflowDef[] workflowDefs = Client.create()
                .resource("http://localhost:" + port + "/api/metadata/workflow")
                .get(WorkflowDef[].class);
        return Arrays.stream(workflowDefs).anyMatch(
                wf -> wf.getName().equalsIgnoreCase("kitchensink")
        );

    }

    /**
     * Enabled JMX reporting:
     * https://docs.newrelic.com/docs/agents/java-agent/troubleshooting/application-server-jmx-setup
     * https://www.eclipse.org/jetty/documentation/current/jmx-chapter
     */
    private void configureMBeanContainer(final Server server){
        final MBeanContainer mbContainer = new MBeanContainer(getPlatformMBeanServer());
        server.addEventListener(mbContainer);
        server.addBean(mbContainer);
        server.addBean(getLog());
    }

    public static void getSecret() {
        System.out.println("Getting secret !!!!!");
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
        } catch (DecryptionFailureException | InternalServiceErrorException | InvalidParameterException | InvalidRequestException | ResourceNotFoundException e) {
            // Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            // Deal with the exception here, and/or rethrow at your discretion.
            logger.error("dis con me !!!", e);
            e.printStackTrace();
            throw e;
        } // An error occurred on the server side.
        // You provided an invalid value for a parameter.
        // You provided a parameter value that is not valid for the current state of the resource.
        // We can't find the resource that you asked for.


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
