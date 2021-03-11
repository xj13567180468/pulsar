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
package org.apache.pulsar;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.create;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Arrays;
import java.util.Optional;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.util.DirectMemoryUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

public class PulsarBrokerStarter {

    private static ServiceConfiguration loadConfig(String configFile) throws Exception {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        ServiceConfiguration config = create((new FileInputStream(configFile)), ServiceConfiguration.class);
        // it validates provided configuration is completed
        isComplete(config);
        return config;
    }

    @VisibleForTesting
    private static class StarterArguments {
        @Parameter(names = {"-c", "--broker-conf"}, description = "Configuration file for Broker")
        private String brokerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/broker.conf";

        @Parameter(names = {"-rb", "--run-bookie"}, description = "Run Bookie together with Broker")
        private boolean runBookie = false;

        @Parameter(names = {"-ra", "--run-bookie-autorecovery"}, description = "Run Bookie Autorecovery together with broker")
        private boolean runBookieAutoRecovery = false;

        @Parameter(names = {"-bc", "--bookie-conf"}, description = "Configuration file for Bookie")
        private String bookieConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/bookkeeper.conf";

        @Parameter(names = {"-rfw", "--run-functions-worker"}, description = "Run functions worker with Broker")
        private boolean runFunctionsWorker = false;

        @Parameter(names = {"-fwc", "--functions-worker-conf"}, description = "Configuration file for Functions Worker")
        private String fnWorkerConfigFile = Paths.get("").toAbsolutePath().normalize().toString() + "/conf/functions_worker.yml";

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;
    }

    private static ServerConfiguration readBookieConfFile(String bookieConfigFile) throws IllegalArgumentException {
        ServerConfiguration bookieConf = new ServerConfiguration();
        try {
            bookieConf.loadConf(new File(bookieConfigFile).toURI().toURL());
            bookieConf.validate();
            log.info("Using bookie configuration file {}", bookieConfigFile);
        } catch (MalformedURLException e) {
            log.error("Could not open configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Could not open configuration file");
        } catch (ConfigurationException e) {
            log.error("Malformed configuration file: {}", bookieConfigFile, e);
            throw new IllegalArgumentException("Malformed configuration file");
        }

        if (bookieConf.getMaxPendingReadRequestPerThread() < bookieConf.getRereplicationEntryBatchSize()) {
            throw new IllegalArgumentException(
                "rereplicationEntryBatchSize should be smaller than " + "maxPendingReadRequestPerThread");
        }
        return bookieConf;
    }

    private static boolean argsContains(String[] args, String arg) {
        return Arrays.asList(args).contains(arg);
    }

    private static class BrokerStarter {
        private final ServiceConfiguration brokerConfig;
        private final PulsarService pulsarService;
        private final BookieServer bookieServer;
        private final AutoRecoveryMain autoRecoveryMain;
        private final StatsProvider bookieStatsProvider;
        private final ServerConfiguration bookieConfig;
        private final WorkerService functionsWorkerService;

        BrokerStarter(String[] args) throws Exception{
            StarterArguments starterArguments = new StarterArguments();
            JCommander jcommander = new JCommander(starterArguments);
            jcommander.setProgramName("PulsarBrokerStarter");

            //JCommander 组件解析命令行
            jcommander.parse(args);
            if (starterArguments.help) {
                jcommander.usage();
                System.exit(-1);
            }

            // 初始化解析命令行
            if (isBlank(starterArguments.brokerConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("Need to specify a configuration file for broker");
            } else {
                //加载broker配置文件
                brokerConfig = loadConfig(starterArguments.brokerConfigFile);
            }

            int maxFrameSize = brokerConfig.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING;
            if (maxFrameSize >= DirectMemoryUtils.maxDirectMemory()) {
                throw new IllegalArgumentException("Max message size need smaller than jvm directMemory");
            }

            if (!NamespaceBundleSplitAlgorithm.availableAlgorithms.containsAll(brokerConfig.getSupportedNamespaceBundleSplitAlgorithms())) {
                throw new IllegalArgumentException("The given supported namespace bundle split algorithm has unavailable algorithm. " +
                    "Available algorithms are " + NamespaceBundleSplitAlgorithm.availableAlgorithms);
            }

            if (!brokerConfig.getSupportedNamespaceBundleSplitAlgorithms().contains(brokerConfig.getDefaultNamespaceBundleSplitAlgorithm())) {
                throw new IllegalArgumentException("Supported namespace bundle split algorithms must contains the default namespace bundle split algorithm");
            }

            // 初始化 functions worker ，判定是否启用函数工作者服务
            if (starterArguments.runFunctionsWorker || brokerConfig.isFunctionsWorkerEnabled()) {
                WorkerConfig workerConfig;
                //初始化函数工作者配置文件
                if (isBlank(starterArguments.fnWorkerConfigFile)) {
                    workerConfig = new WorkerConfig();
                } else {
                    workerConfig = WorkerConfig.load(starterArguments.fnWorkerConfigFile);
                }
                // 配置工作者与本地 broker 通信
                String hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(
                    brokerConfig.getAdvertisedAddress());
                workerConfig.setWorkerHostname(hostname);
                workerConfig.setWorkerPort(brokerConfig.getWebServicePort().get());
                workerConfig.setWorkerId(
                    "c-" + brokerConfig.getClusterName()
                        + "-fw-" + hostname
                        + "-" + workerConfig.getWorkerPort());
                // 代理授权设置
                workerConfig.setAuthenticationEnabled(brokerConfig.isAuthenticationEnabled());
                workerConfig.setAuthenticationProviders(brokerConfig.getAuthenticationProviders());

                workerConfig.setAuthorizationEnabled(brokerConfig.isAuthorizationEnabled());
                workerConfig.setAuthorizationProvider(brokerConfig.getAuthorizationProvider());
                workerConfig.setConfigurationStoreServers(brokerConfig.getConfigurationStoreServers());
                workerConfig.setZooKeeperSessionTimeoutMillis(brokerConfig.getZooKeeperSessionTimeoutMillis());
                workerConfig.setZooKeeperOperationTimeoutSeconds(brokerConfig.getZooKeeperOperationTimeoutSeconds());

                workerConfig.setTlsHostnameVerificationEnable(false);

                workerConfig.setTlsAllowInsecureConnection(brokerConfig.isTlsAllowInsecureConnection());
                workerConfig.setTlsTrustCertsFilePath(brokerConfig.getTlsTrustCertsFilePath());

                // client in worker will use this config to authenticate with broker
                workerConfig.setClientAuthenticationPlugin(brokerConfig.getBrokerClientAuthenticationPlugin());
                workerConfig.setClientAuthenticationParameters(brokerConfig.getBrokerClientAuthenticationParameters());

                // inherit super users
                workerConfig.setSuperUserRoles(brokerConfig.getSuperUserRoles());

                functionsWorkerService = new WorkerService(workerConfig);
            } else {
                //意味着不启用
                functionsWorkerService = null;
            }

            // 初始化pulsar服务
            pulsarService = new PulsarService(brokerConfig, Optional.ofNullable(functionsWorkerService));

            // 如果在命令行中没有运行 bookie （命令），则尝试从 pulsar 配置文件中读取
            if (!argsContains(args, "-rb") && !argsContains(args, "--run-bookie")) {
                checkState(starterArguments.runBookie == false,
                    "runBookie should be false if has no argument specified");
                starterArguments.runBookie = brokerConfig.isEnableRunBookieTogether();
            }
            // 加载顺序同上，表示是否启用 bookie 自动恢复服务
            if (!argsContains(args, "-ra") && !argsContains(args, "--run-bookie-autorecovery")) {
                checkState(starterArguments.runBookieAutoRecovery == false,
                    "runBookieAutoRecovery should be false if has no argument specified");
                starterArguments.runBookieAutoRecovery = brokerConfig.isEnableRunBookieAutoRecoveryTogether();
            }
            //从这里可以看出，要么运行 bookie（读写） 服务，要么运行 bookie 自动恢复服务
            if ((starterArguments.runBookie || starterArguments.runBookieAutoRecovery)
                && isBlank(starterArguments.bookieConfigFile)) {
                jcommander.usage();
                throw new IllegalArgumentException("No configuration file for Bookie");
            }

            // 初始化 bookie 状态提供者
            if (starterArguments.runBookie || starterArguments.runBookieAutoRecovery) {
                checkState(isNotBlank(starterArguments.bookieConfigFile),
                    "No configuration file for Bookie");
                bookieConfig = readBookieConfFile(starterArguments.bookieConfigFile);
                Class<? extends StatsProvider> statsProviderClass = bookieConfig.getStatsProviderClass();
                bookieStatsProvider = ReflectionUtils.newInstance(statsProviderClass);
            } else {
                bookieConfig = null;
                bookieStatsProvider = null;
            }

            // // 如果配置文件设置当前服务器运行 bookie 服务，则初始化 bookie 服务器
            if (starterArguments.runBookie) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie");
                checkNotNull(bookieStatsProvider, "No Stats Provider for Bookie");
                bookieServer = new BookieServer(
                        bookieConfig, bookieStatsProvider.getStatsLogger(""), BookieServiceInfo.NO_INFO);
            } else {
                bookieServer = null;
            }

            // 如果设置 bookie 自动恢复标志，则初始化 bookie 自动恢复服务
            if (starterArguments.runBookieAutoRecovery) {
                checkNotNull(bookieConfig, "No ServerConfiguration for Bookie Autorecovery");
                autoRecoveryMain = new AutoRecoveryMain(bookieConfig);
            } else {
                autoRecoveryMain = null;
            }
        }

        public void start() throws Exception {
            if (bookieStatsProvider != null) {
                bookieStatsProvider.start(bookieConfig);
                log.info("started bookieStatsProvider.");
            }
            if (bookieServer != null) {
                bookieServer.start();
                log.info("started bookieServer.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.start();
                log.info("started bookie autoRecoveryMain.");
            }

            pulsarService.start();
            log.info("PulsarService started.");
        }

        public void join() throws InterruptedException {
            pulsarService.waitUntilClosed();

            try {
                pulsarService.close();
            } catch (PulsarServerException e) {
                throw new RuntimeException();
            }

            if (bookieServer != null) {
                bookieServer.join();
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.join();
            }
        }

        public void shutdown() {
            if (null != functionsWorkerService) {
                functionsWorkerService.stop();
                log.info("Shut down functions worker service successfully.");
            }

            pulsarService.getShutdownService().run();
            log.info("Shut down broker service successfully.");

            if (bookieStatsProvider != null) {
                bookieStatsProvider.stop();
                log.info("Shut down bookieStatsProvider successfully.");
            }
            if (bookieServer != null) {
                bookieServer.shutdown();
                log.info("Shut down bookieServer successfully.");
            }
            if (autoRecoveryMain != null) {
                autoRecoveryMain.shutdown();
                log.info("Shut down autoRecoveryMain successfully.");
            }
        }
    }


    public static void main(String[] args) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
        //设置线程异常未catch情况下默认处理方法
        Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
            System.out.println(String.format("%s [%s] error Uncaught exception in thread %s: %s", dateFormat.format(new Date()), thread.getContextClassLoader(), thread.getName(), exception.getMessage()));
        });
        //创建 broker 启动器实例
        BrokerStarter starter = new BrokerStarter(args);
        //安装JVM退出时钩子，即调用 broker 启动器关闭
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                starter.shutdown();
            })
        );

        PulsarByteBufAllocator.registerOOMListener(oomException -> {
            log.error("-- Shutting down - Received OOM exception: {}", oomException.getMessage(), oomException);
            starter.shutdown();
        });

        try {
            //调用启动器启动
            starter.start();
        } catch (Exception e) {
            log.error("Failed to start pulsar service.", e);
            Runtime.getRuntime().halt(1);
        } finally {
            //等待启动器退出
            starter.join();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarBrokerStarter.class);
}
