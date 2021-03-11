/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A service component contains everything to run a worker except rest server.
 */
@Slf4j
@Getter
public class WorkerService {
    //工作者配置
    private final WorkerConfig workerConfig;
    //Pulsar 客户端
    private PulsarClient client;
    //函数运行时管理器
    private FunctionRuntimeManager functionRuntimeManager;
    //函数元数据管理器
    private FunctionMetaDataManager functionMetaDataManager;
    //集群服务协调器
    private ClusterServiceCoordinator clusterServiceCoordinator;
    // DistributedLog Namespace 用于存储函数jars
    private Namespace dlogNamespace;
    // 存储客户端，函数用于访问状态存储器
    private StorageAdminClient stateStoreAdminClient;
    //成员管理器
    private MembershipManager membershipManager;
    //调度管理器
    private SchedulerManager schedulerManager;
    //是否已初始化
    private boolean isInitialized = false;
    //用于状态更新的定时服务
    private final ScheduledExecutorService statsUpdater;
    //认证服务
    private AuthenticationService authenticationService;
    //连接管理器
    private AuthorizationService authorizationService;
    //Pulsar admin 客户端
    private ConnectorsManager connectorsManager;
    //函数管理端
    private PulsarAdmin brokerAdmin;
    //指标生成器
    private PulsarAdmin functionAdmin;
    //指标生成器
    private final MetricsGenerator metricsGenerator;
    //定时执行器
    private final ScheduledExecutorService executor;
    //log存储路径
    @VisibleForTesting
    private URI dlogUri;

    public WorkerService(WorkerConfig workerConfig) {
        this.workerConfig = workerConfig;
        this.statsUpdater = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("worker-stats-updater"));
        this.executor = Executors.newScheduledThreadPool(10, new DefaultThreadFactory("pulsar-worker"));
        this.metricsGenerator = new MetricsGenerator(this.statsUpdater, workerConfig);
    }


    public void start(URI dlogUri,
                      AuthenticationService authenticationService,
                      AuthorizationService authorizationService) throws InterruptedException {
        log.info("Starting worker {}...", workerConfig.getWorkerId());

        try {
            log.info("Worker Configs: {}", new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValueAsString(workerConfig));
        } catch (JsonProcessingException e) {
            log.warn("Failed to print worker configs with error {}", e.getMessage(), e);
        }

        try {
            // create the dlog namespace for storing function packages
            this.dlogUri = dlogUri;
            DistributedLogConfiguration dlogConf = WorkerUtils.getDlogConf(workerConfig);
            try {
                this.dlogNamespace = NamespaceBuilder.newBuilder()
                        .conf(dlogConf)
                        .clientId("function-worker-" + workerConfig.getWorkerId())
                        .uri(this.dlogUri)
                        .build();
            } catch (Exception e) {
                log.error("Failed to initialize dlog namespace {} for storing function packages",
                        dlogUri, e);
                throw new RuntimeException(e);
            }

            // create the state storage client for accessing function state
            if (workerConfig.getStateStorageServiceUrl() != null) {
                StorageClientSettings clientSettings = StorageClientSettings.newBuilder()
                        .serviceUri(workerConfig.getStateStorageServiceUrl())
                        .build();
                this.stateStoreAdminClient = StorageClientBuilder.newBuilder()
                        .withSettings(clientSettings)
                        .buildAdmin();
            }

            final String functionWebServiceUrl = StringUtils.isNotBlank(workerConfig.getFunctionWebServiceUrl())
                    ? workerConfig.getFunctionWebServiceUrl()
                    : workerConfig.getWorkerWebAddress();

            if (workerConfig.isAuthenticationEnabled()) {
                this.brokerAdmin = WorkerUtils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl(),
                    workerConfig.getClientAuthenticationPlugin(), workerConfig.getClientAuthenticationParameters(),
                    workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection(),
                    workerConfig.isTlsHostnameVerificationEnable());

                this.functionAdmin = WorkerUtils.getPulsarAdminClient(functionWebServiceUrl,
                    workerConfig.getClientAuthenticationPlugin(), workerConfig.getClientAuthenticationParameters(),
                    workerConfig.getTlsTrustCertsFilePath(), workerConfig.isTlsAllowInsecureConnection(),
                    workerConfig.isTlsHostnameVerificationEnable());

                this.client = WorkerUtils.getPulsarClient(this.workerConfig.getPulsarServiceUrl(),
                        workerConfig.getClientAuthenticationPlugin(),
                        workerConfig.getClientAuthenticationParameters(),
                        workerConfig.isUseTls(), workerConfig.getTlsTrustCertsFilePath(),
                        workerConfig.isTlsAllowInsecureConnection(), workerConfig.isTlsHostnameVerificationEnable());
            } else {
                this.brokerAdmin = WorkerUtils.getPulsarAdminClient(workerConfig.getPulsarWebServiceUrl());

                this.functionAdmin = WorkerUtils.getPulsarAdminClient(functionWebServiceUrl);

                this.client = WorkerUtils.getPulsarClient(this.workerConfig.getPulsarServiceUrl());
            }
            log.info("Created Pulsar client");

            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getFunctionAssignmentTopic());
            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getClusterCoordinationTopic());
            brokerAdmin.topics().createNonPartitionedTopic(workerConfig.getFunctionMetadataTopic());
            //create scheduler manager
            this.schedulerManager = new SchedulerManager(this.workerConfig, this.client, this.brokerAdmin,
                    this.executor);

            //create function meta data manager
            this.functionMetaDataManager = new FunctionMetaDataManager(
                    this.workerConfig, this.schedulerManager, this.client);

            this.connectorsManager = new ConnectorsManager(workerConfig);

            //create membership manager
            String coordinationTopic = workerConfig.getClusterCoordinationTopic();
            if (!brokerAdmin.topics().getSubscriptions(coordinationTopic).contains(MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION)) {
                brokerAdmin.topics().createSubscription(coordinationTopic, MembershipManager.COORDINATION_TOPIC_SUBSCRIPTION, MessageId.earliest);
            }
            this.membershipManager = new MembershipManager(this, this.client, this.brokerAdmin);

            // create function runtime manager
            this.functionRuntimeManager = new FunctionRuntimeManager(
                    this.workerConfig, this, this.dlogNamespace, this.membershipManager, connectorsManager, functionMetaDataManager);

            // Setting references to managers in scheduler
            this.schedulerManager.setFunctionMetaDataManager(this.functionMetaDataManager);
            this.schedulerManager.setFunctionRuntimeManager(this.functionRuntimeManager);
            this.schedulerManager.setMembershipManager(this.membershipManager);

            // initialize function metadata manager
            this.functionMetaDataManager.initialize();

            // initialize function runtime manager
            this.functionRuntimeManager.initialize();

            this.authenticationService = authenticationService;

            this.authorizationService = authorizationService;

            // Starting cluster services
            log.info("Start cluster services...");
            this.clusterServiceCoordinator = new ClusterServiceCoordinator(
                    this.workerConfig.getWorkerId(),
                    membershipManager);

            this.clusterServiceCoordinator.addTask("membership-monitor",
                    this.workerConfig.getFailureCheckFreqMs(),
                    () -> membershipManager.checkFailures(
                            functionMetaDataManager, functionRuntimeManager, schedulerManager));

            this.clusterServiceCoordinator.start();

            // Start function runtime manager
            this.functionRuntimeManager.start();

            // indicate function worker service is done initializing
            this.isInitialized = true;
        } catch (Throwable t) {
            log.error("Error Starting up in worker", t);
            throw new RuntimeException(t);
        }
    }

    public void stop() {
        if (null != functionMetaDataManager) {
            try {
                functionMetaDataManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function metadata manager", e);
            }
        }
        if (null != functionRuntimeManager) {
            try {
                functionRuntimeManager.close();
            } catch (Exception e) {
                log.warn("Failed to close function runtime manager", e);
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close pulsar client", e);
            }
        }

        if (null != clusterServiceCoordinator) {
            clusterServiceCoordinator.close();
        }

        if (null != membershipManager) {
            try {
                membershipManager.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close membership manager", e);
            }
        }

        if (null != schedulerManager) {
            schedulerManager.close();
        }

        if (null != this.brokerAdmin) {
            this.brokerAdmin.close();
        }

        if (null != this.functionAdmin) {
            this.functionAdmin.close();
        }

        if (null != this.stateStoreAdminClient) {
            this.stateStoreAdminClient.close();
        }

        if (null != this.dlogNamespace) {
            this.dlogNamespace.close();
        }

        if(this.executor != null) {
            this.executor.shutdown();
        }

        if (this.statsUpdater != null) {
            statsUpdater.shutdownNow();
        }
    }

}
