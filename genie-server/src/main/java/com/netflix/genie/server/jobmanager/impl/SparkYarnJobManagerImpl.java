/*
 *
 *  Copyright 2015 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.genie.server.jobmanager.impl;

import com.netflix.config.ConfigurationManager;
import com.netflix.genie.common.exceptions.GenieException;
import com.netflix.genie.common.exceptions.GeniePreconditionException;
import com.netflix.genie.common.exceptions.GenieServerException;
import com.netflix.genie.server.jobmanager.JobMonitor;
import com.netflix.genie.server.services.CommandConfigService;
import com.netflix.genie.server.services.JobService;
import com.netflix.genie.server.util.StringUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of job manager for Spark Jobs.
 */
@Named
@Scope("prototype")
public class SparkYarnJobManagerImpl extends YarnJobManagerImpl {

    private static final Logger LOG = LoggerFactory.getLogger(SparkYarnJobManagerImpl.class);
    private static final String SPARK_DEPLOY_MODE = "com.netflix.genie.server.job.manager.spark.deploy.mode";
    private static final String SPARK_DRIVER_DEP_JARS = "com.netflix.genie.server.job.manager.spark.driver.dep.jars";
    private static final String SPARK_DRIVER_JAVA_OPTS = "com.netflix.genie.server.job.manager.spark.driver.java.opts";
    private static final String SPARK_DRIVER_MEM = "com.netflix.genie.server.job.manager.spark.driver.memory";
    private static final String SPARK_HS_ADDR = "com.netflix.genie.server.job.manager.spark.hs.address";

    /**
     * Default constructor - initializes cluster configuration and load
     * balancer.
     *
     * @param jobMonitor     The job monitor object to use.
     * @param jobService     The job service to use.
     * @param commandService The command service to use.
     */
    @Inject
    public SparkYarnJobManagerImpl(final JobMonitor jobMonitor,
                                   final JobService jobService,
                                   final CommandConfigService commandService) {
        super(jobMonitor, jobService, commandService);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void launch() throws GenieException {
        LOG.info("called");
        if (!this.isInitCalled()) {
            throw new GeniePreconditionException("Init wasn't called. Unable to continue.");
        }

        // Check the parameters
        final String deployMode = ConfigurationManager
                .getConfigInstance().getString(SPARK_DEPLOY_MODE, "cluster");
        final String driverDepJars = ConfigurationManager
                .getConfigInstance().getString(SPARK_DRIVER_DEP_JARS, null);
        final String driverJavaOpts = ConfigurationManager
                .getConfigInstance().getString(SPARK_DRIVER_JAVA_OPTS, null);
        final String driverMem = ConfigurationManager
                .getConfigInstance().getString(SPARK_DRIVER_MEM, null);
        String sparkHSAddr = ConfigurationManager
                .getConfigInstance().getString(SPARK_HS_ADDR, null);
        if (sparkHSAddr == null) {
            LOG.warn("Spark HS address not set. Assuming it is running on the master node of YARN cluster.");
            XMLConfiguration yarnConf;
            try {
                yarnConf = new XMLConfiguration(this.getJobDir() + "/conf/yarn-site.xml");
            } catch (ConfigurationException e) {
                throw new GenieServerException(e);
            }
            String yarnRMAddr = (String) yarnConf.getRoot().getChildren("yarn.resourcemanager.address").get(0).getValue();
            LOG.info("YARN RM address: " + yarnRMAddr);
            String yarnRMHost = yarnRMAddr.substring(0, yarnRMAddr.indexOf(":"));
            sparkHSAddr = yarnRMHost + ":18080";
            LOG.info("Spark HS address: " + sparkHSAddr);
        }

        // create the ProcessBuilder for this process
        final List<String> processArgs = this.createBaseProcessArguments();
        processArgs.add("--deploy-mode");
        processArgs.add(deployMode);
        if (driverDepJars != null) {
            processArgs.add("--jars");
            processArgs.add(driverDepJars);
        }
        if (driverJavaOpts != null) {
            processArgs.add("--driver-java-options");
            processArgs.add(driverJavaOpts);
        }
        if (driverMem != null) {
            processArgs.add("--driver-memory");
            processArgs.add(driverMem);
        }
        processArgs.add("--conf");
        processArgs.add("spark.yarn.historyServer.address=" + sparkHSAddr);
        processArgs.addAll(Arrays.asList(StringUtil.splitCmdLine(this.getJob().getCommandArgs())));

        final ProcessBuilder processBuilder = new ProcessBuilder(processArgs);

        // construct the environment variables
        this.setupCommonProcess(processBuilder);
        super.setupYarnProcess(processBuilder);

        // Launch the actual process
        this.launchProcess(
                processBuilder,
                ConfigurationManager
                        .getConfigInstance()
                        .getInt("com.netflix.genie.server.job.manager.spark.sleeptime", 5000)
        );
    }
}
