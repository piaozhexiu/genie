/*
 *
\ *  Copyright 2014 Netflix, Inc.
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
package com.netflix.genie.common.model;

import com.netflix.genie.common.exceptions.GenieException;
import com.wordnik.swagger.annotations.ApiModelProperty;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Lob;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of a Genie 2.0 job.
 *
 * @author amsharma
 * @author tgianos
 */
@Entity
@Cacheable(false)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class Job extends CommonEntityFields {

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);
    private static final char CRITERIA_SET_DELIMITER = '|';
    private static final char CRITERIA_DELIMITER = ',';
    private static final String DEFAULT_VERSION = "-1";

    // ------------------------------------------------------------------------
    // GENERAL COMMON PARAMS FOR ALL JOBS - TO BE SPECIFIED BY CLIENTS
    // ------------------------------------------------------------------------
    /**
     * Command line arguments (REQUIRED).
     */
    @Lob
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Command line arguments for the job",
            required = true)
    private String commandArgs;

    /**
     * Human readable description.
     */
    @Basic
    @ApiModelProperty(
            value = "Description specified for the job")
    private String description;

    /**
     * The group user belongs.
     */
    @Basic
    @Column(name = "groupName")
    @ApiModelProperty(
            value = "group name of the user who submitted this job")
    private String group;

    /**
     * Alias - Cluster Name of the cluster selected to run the job.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the cluster where the job is run")
    private String executionClusterName;

    /**
     * ID for the cluster that was selected to run the job .
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the cluster where the job is run")
    private String executionClusterId;

    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    @ApiModelProperty(
            value = "Path to a shell file which is sourced before job is run.")
    private String envPropFile;

    /**
     * Set of tags to use for scheduling (REQUIRED).
     */
    @Transient
    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a cluster to run this job")
    private List<ClusterCriteria> clusterCriteria;

    /**
     * Set of tags to use for selecting command (REQUIRED).
     */
    @Transient
    @ApiModelProperty(
            value = "List of criteria containing tags to use to pick a command to run this job")
    private Set<String> commandCriteria;

    /**
     * String representation of the the cluster criteria array list object
     * above.
     */
    @XmlTransient
    @JsonIgnore
    @Lob
    @Basic(optional = false)
    private String clusterCriteriaString;

    /**
     * String representation of the the command criteria set object
     * above.
     */
    @XmlTransient
    @JsonIgnore
    @Lob
    @Basic(optional = false)
    private String commandCriteriaString;

    /**
     * String representation of the criteria that was successfully
     * used to select a cluster.
     */
    @XmlTransient
    @JsonIgnore
    @Lob
    @Basic
    private String chosenClusterCriteriaString;

    /**
     * File dependencies.
     */
    @Lob
    @ApiModelProperty(
            value = "Dependent files for this job to run.")
    private String fileDependencies;

    /**
     * Set of file dependencies, sent as MIME attachments. This is not persisted
     * in the DB for space reasons.
     */
    @Transient
    @ApiModelProperty(
            value = "Attachments sent as a part of job request.")
    private Set<FileAttachment> attachments;

    /**
     * Whether to disable archive logs or not - default is false.
     */
    @Basic
    @ApiModelProperty(
            value = "Boolean variable to decide whether job should be archived after it finishes.")
    private boolean disableLogArchival;

    /**
     * Email address of the user where they expects an email. This is sent once
     * the Genie job completes.
     */
    @Basic
    @ApiModelProperty(
            value = "Email address to send notifications to on job completion.")
    private String email;

    /**
     * Set of tags for a job.
     */
    @XmlElementWrapper(name = "tags")
    @XmlElement(name = "tag")
    @ElementCollection(fetch = FetchType.EAGER)
    @ApiModelProperty(
            value = "Reference to all the tags"
                    + " associated with this job.")
    private Set<String> tags;

    // ------------------------------------------------------------------------
    // Genie2 command and application combinations to be specified by the user while running jobs.
    // ------------------------------------------------------------------------
    /**
     * Application name - e.g. mapreduce, tez
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the application that this job should use to run.")
    private String applicationName;

    /**
     * Application Id to pin to specific application id e.g. mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the application that this job should use to run.")
    private String applicationId;

    /**
     * Command name to run - e.g. prodhive, testhive, prodpig, testpig.
     */
    @Basic
    @ApiModelProperty(
            value = "Name of the command that this job should run.")
    private String commandName;

    /**
     * Command Id to run - Used to pin to a particular command e.g.
     * prodhive11_mr1
     */
    @Basic
    @ApiModelProperty(
            value = "Id of the command that this job should run.")
    private String commandId;

    // ------------------------------------------------------------------------
    // GENERAL COMMON STUFF FOR ALL JOBS
    // TO BE GENERATED/USED BY SERVER
    // ------------------------------------------------------------------------
    /**
     * PID for job - updated by the server.
     */
    @Basic
    @ApiModelProperty(
            value = "The process handle.")
    private int processHandle = -1;

    /**
     * Job status - INIT, RUNNING, SUCCEEDED, KILLED, FAILED (upper case in DB).
     */
    @Basic
    @Enumerated(EnumType.STRING)
    @ApiModelProperty(
            value = "The current status of the job.")
    private JobStatus status;

    /**
     * More verbose status message.
     */
    @Basic
    @ApiModelProperty(
            value = "A status message about the job.")
    private String statusMsg;

    /**
     * Start time for job - initialized to null.
     */
    @Basic
    @ApiModelProperty(
            value = "The start time of the job.")
    private long startTime = -1L;

    /**
     * Finish time for job - initialized to zero (for historic reasons).
     */
    @Basic
    @ApiModelProperty(
            value = "The end time of the job.")
    private long finishTime = 0L;

    /**
     * The host/IP address of the client submitting job.
     */
    @Basic
    @ApiModelProperty(
            value = "The host of the client submitting the job.")
    private String clientHost;

    /**
     * The genie host name on which the job is being run.
     */
    @Basic
    @ApiModelProperty(
            value = "The host where the job is being run.")
    private String hostName;

    /**
     * REST URI to do a HTTP DEL on to kill this job - points to running
     * instance.
     */
    @Basic
    @ApiModelProperty(
            value = "The URI to use to kill the job.")
    private String killURI;

    /**
     * URI to fetch the stdout/err and logs.
     */
    @Basic
    @ApiModelProperty(
            value = "The URI where to find job output.")
    private String outputURI;

    /**
     * Job exit code.
     */
    @Basic
    @ApiModelProperty(
            value = "The exit code of the job.")
    private int exitCode = -1;

    /**
     * Whether this job was forwarded to new instance or not.
     */
    @Basic
    @ApiModelProperty(
            value = "Whether this job was forwared or not.")
    private boolean forwarded;

    /**
     * Location of logs being archived to s3.
     */
    @Lob
    @ApiModelProperty(
            value = "Where the logs were archived in S3.")
    private String archiveLocation;

    /**
     * Default Constructor.
     */
    public Job() {
        super();
    }

    /**
     * Construct a new Job.
     *
     * @param user            The name of the user running the job. Not null/empty/blank.
     * @param commandArgs     The command line arguments for the job. Not
     *                        null/empty/blank.
     * @param commandCriteria The criteria for the command. Not null/empty.
     * @param clusterCriteria The cluster criteria for the job. Not null/empty.
     * @param version         The version of this job
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    public Job(
            final String user,
            final String name,
            final String commandArgs,
            final Set<String> commandCriteria,
            final List<ClusterCriteria> clusterCriteria,
            final String version) throws GenieException {
        super(name, user, version);

        this.commandArgs = commandArgs;
        this.clusterCriteria = clusterCriteria;
        this.commandCriteria = commandCriteria;

        if (StringUtils.isBlank(this.getVersion())) {
            this.setVersion(DEFAULT_VERSION);
        }
    }

    /**
     * Makes sure non-transient fields are set from transient fields.
     *
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    @PrePersist
    protected void onCreateJob() throws GenieException {
        this.validate(this.commandCriteria, this.commandArgs, this.clusterCriteria);
        this.clusterCriteriaString = clusterCriteriaToString(this.clusterCriteria);
        this.commandCriteriaString = commandCriteriaToString(this.commandCriteria);
        // Add the id to the tags
        if (this.tags == null) {
            this.tags = new HashSet<String>();
        }
        this.tags.add(this.getId());
    }

    /**
     * On any update to the entity will add id to tags.
     */
    @PreUpdate
    protected void onUpdateJob() {
        // Add the id to the tags
        this.tags.add(this.getId());
    }

    /**
     * Gets the description of this job.
     *
     * @return description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Sets the description for this job.
     *
     * @param description description for the job
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * Gets the group name of the user who submitted the job.
     *
     * @return group
     */
    public String getGroup() {
        return this.group;
    }

    /**
     * Sets the group of the user who submits the job.
     *
     * @param group group of the user submitting the job
     */
    public void setGroup(final String group) {
        this.group = group;
    }

    /**
     * Gets the name of the cluster on which this job was run.
     *
     * @return executionClusterName
     */
    public String getExecutionClusterName() {
        return this.executionClusterName;
    }

    /**
     * Sets the name of the cluster on which this job is run.
     *
     * @param executionClusterName Name of the cluster on which job is executed.
     *                             Populated by the server.
     */
    public void setExecutionClusterName(final String executionClusterName) {
        this.executionClusterName = executionClusterName;
    }

    /**
     * Gets the id of the cluster on which this job was run.
     *
     * @return executionClusterId
     */
    public String getExecutionClusterId() {
        return this.executionClusterId;
    }

    /**
     * Sets the id of the cluster on which this job is run.
     *
     * @param executionClusterId Id of the cluster on which job is executed.
     *                           Populated by the server.
     */
    public void setExecutionClusterId(final String executionClusterId) {
        this.executionClusterId = executionClusterId;
    }

    /**
     * Gets the cluster criteria which was specified to pick a cluster to run the job.
     *
     * @return clusterCriteria
     */
    public List<ClusterCriteria> getClusterCriteria() {
        return this.clusterCriteria;
    }

    /**
     * Sets the list of cluster criteria specified to pick a cluster.
     *
     * @param clusterCriteria The criteria list
     * @throws GenieException
     */
    public void setClusterCriteria(final List<ClusterCriteria> clusterCriteria) throws GenieException {
        if (clusterCriteria == null || clusterCriteria.isEmpty()) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No user entered.");
        }
        this.clusterCriteria = clusterCriteria;
        this.clusterCriteriaString = clusterCriteriaToString(clusterCriteria);
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getCommandArgs() {
        return this.commandArgs;
    }

    /**
     * Parameters specified to be run and fed as command line arguments to the
     * job run.
     *
     * @param commandArgs Arguments to be used to run the command with. Not
     *                    null/empty/blank.
     * @throws GenieException
     */
    public void setCommandArgs(final String commandArgs) throws GenieException {
        if (StringUtils.isBlank(commandArgs)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command args entered.");
        }
        this.commandArgs = commandArgs;
    }

    /**
     * Gets the fileDependencies for the job.
     *
     * @return fileDependencies
     */
    public String getFileDependencies() {
        return this.fileDependencies;
    }

    /**
     * Sets the fileDependencies for the job.
     *
     * @param fileDependencies Dependent files for the job in csv format
     */
    public void setFileDependencies(final String fileDependencies) {
        this.fileDependencies = fileDependencies;
    }

    /**
     * Get the attachments for this job.
     *
     * @return The attachments
     */
    public Set<FileAttachment> getAttachments() {
        return this.attachments;
    }

    /**
     * Set the attachments for this job.
     *
     * @param attachments The attachments to set
     */
    public void setAttachments(final Set<FileAttachment> attachments) {
        this.attachments = attachments;
    }

    /**
     * Is the log archival disabled.
     *
     * @return true if it's disabled
     */
    public boolean isDisableLogArchival() {
        return this.disableLogArchival;
    }

    /**
     * Set whether the log archival is disabled or not.
     *
     * @param disableLogArchival True if disabling is desired
     */
    public void setDisableLogArchival(final boolean disableLogArchival) {
        this.disableLogArchival = disableLogArchival;
    }

    /**
     * Gets the commandArgs specified to run the job.
     *
     * @return commandArgs
     */
    public String getEmail() {
        return this.email;
    }

    /**
     * Set user Email address for the job.
     *
     * @param email user email address
     */
    public void setEmail(final String email) {
        this.email = email;
    }

    /**
     * Gets the application name specified to run the job.
     *
     * @return applicationName
     */
    public String getApplicationName() {
        return this.applicationName;
    }

    /**
     * Set application Name with which this job is run, if not null.
     *
     * @param applicationName Name of the application if specified on which the
     *                        job is run
     */
    public void setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
    }

    /**
     * Gets the application id specified to run the job.
     *
     * @return applicationId
     */
    public String getApplicationId() {
        return this.applicationId;
    }

    /**
     * Set application Id with which this job is run, if not null.
     *
     * @param applicationId Id of the application if specified on which the job
     *                      is run
     */
    public void setApplicationId(final String applicationId) {
        this.applicationId = applicationId;
    }

    /**
     * Gets the command name for this job.
     *
     * @return commandName
     */
    public String getCommandName() {
        return this.commandName;
    }

    /**
     * Set command Name with which this job is run.
     *
     * @param commandName Name of the command if specified on which the job is
     *                    run
     * @throws GenieException
     */
    public void setCommandName(final String commandName) throws GenieException {
        if (StringUtils.isBlank(commandName)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command name entered.");
        }
        this.commandName = commandName;
    }

    /**
     * Gets the command id for this job.
     *
     * @return commandId
     */
    public String getCommandId() {
        return this.commandId;
    }

    /**
     * Set command Id with which this job is run.
     *
     * @param commandId Id of the command if specified on which the job is run
     * @throws com.netflix.genie.common.exceptions.GenieException
     */
    public void setCommandId(final String commandId) throws GenieException {
        if (StringUtils.isBlank(commandId)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command id entered.");
        }
        this.commandId = commandId;
    }

    /**
     * Get the process handle for the job.
     *
     * @return processHandle
     */
    public int getProcessHandle() {
        return this.processHandle;
    }

    /**
     * Set the process handle for the job.
     *
     * @param processHandle the process handle
     */
    public void setProcessHandle(final int processHandle) {
        this.processHandle = processHandle;
    }

    /**
     * Gets the status for this job.
     *
     * @return status
     * @see JobStatus
     */
    public JobStatus getStatus() {
        return this.status;
    }

    /**
     * Set the status for the job.
     *
     * @param status The new status
     */
    public void setStatus(final JobStatus status) {
        this.status = status;
    }

    /**
     * Gets the status message or this job.
     *
     * @return statusMsg
     */
    public String getStatusMsg() {
        return this.statusMsg;
    }

    /**
     * Set the status message for the job.
     *
     * @param statusMsg The status message.
     */
    public void setStatusMsg(final String statusMsg) {
        this.statusMsg = statusMsg;
    }

    /**
     * Gets the start time for this job.
     *
     * @return startTime : start time in ms
     */
    public long getStartTime() {
        return this.startTime;
    }

    /**
     * Set the startTime for the job.
     *
     * @param startTime epoch time in ms
     */
    public void setStartTime(final long startTime) {
        this.startTime = startTime;
    }

    /**
     * Gets the finish time for this job.
     *
     * @return finishTime
     */
    public long getFinishTime() {
        return this.finishTime;
    }

    /**
     * Set the finishTime for the job.
     *
     * @param finishTime epoch time in ms
     */
    public void setFinishTime(final long finishTime) {
        this.finishTime = finishTime;
    }

    /**
     * Gets client hostname from which this job is run.
     *
     * @return clientHost
     */
    public String getClientHost() {
        return this.clientHost;
    }

    /**
     * Set the client host for the job.
     *
     * @param clientHost
     */
    public void setClientHost(final String clientHost) {
        this.clientHost = clientHost;
    }

    /**
     * Gets genie hostname on which this job is run.
     *
     * @return hostName
     */
    public String getHostName() {
        return this.hostName;
    }

    /**
     * Set the genie hostname on which the job is run.
     *
     * @param hostName
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Get the kill URI for this job.
     *
     * @return killURI
     */
    public String getKillURI() {
        return this.killURI;
    }

    /**
     * Set the kill URI for this job.
     *
     * @param killURI The kill URI
     */
    public void setKillURI(final String killURI) {
        this.killURI = killURI;
    }

    /**
     * Get the output URI for this job.
     *
     * @return outputURI
     */
    public String getOutputURI() {
        return this.outputURI;
    }

    /**
     * Set the output URI for this job.
     *
     * @param outputURI The output URI.
     */
    public void setOutputURI(final String outputURI) {
        this.outputURI = outputURI;
    }

    /**
     * Get the exit code for this job.
     *
     * @return exitCode The exit code. 0 for Success.
     */
    public int getExitCode() {
        return this.exitCode;
    }

    /**
     * Set the exit code for this job.
     *
     * @param exitCode
     */
    public void setExitCode(final int exitCode) {
        this.exitCode = exitCode;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @return true, if forwarded
     */
    public boolean isForwarded() {
        return this.forwarded;
    }

    /**
     * Has the job been forwarded to another instance.
     *
     * @param forwarded true, if forwarded
     */
    public void setForwarded(final boolean forwarded) {
        this.forwarded = forwarded;
    }

    /**
     * Get location where logs are archived.
     *
     * @return s3/hdfs location where logs are archived
     */
    public String getArchiveLocation() {
        return this.archiveLocation;
    }

    /**
     * Set location where logs are archived.
     *
     * @param archiveLocation s3/hdfs location where logs are archived
     */
    public void setArchiveLocation(final String archiveLocation) {
        this.archiveLocation = archiveLocation;
    }

    /**
     * Get the cluster criteria specified to run this job in string format.
     *
     * @return clusterCriteriaString
     */
    protected String getClusterCriteriaString() {
        return this.clusterCriteriaString;
    }

    /**
     * Set the cluster criteria string.
     *
     * @param clusterCriteriaString A list of cluster criteria objects
     * @throws GenieException
     */
    protected void setClusterCriteriaString(final String clusterCriteriaString) throws GenieException {
        if (StringUtils.isBlank(clusterCriteriaString)) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No clusterCriteriaString passed in to set. Unable to continue.");
        }
        this.clusterCriteriaString = clusterCriteriaString;
        this.clusterCriteria = stringToClusterCriteria(clusterCriteriaString);
    }

    /**
     * Gets the command criteria which was specified to pick a command to run the job.
     *
     * @return commandCriteria
     */
    public Set<String> getCommandCriteria() {
        return this.commandCriteria;
    }

    /**
     * Sets the set of command criteria specified to pick a command.
     *
     * @param commandCriteria The criteria list
     * @throws GenieException
     */
    public void setCommandCriteria(Set<String> commandCriteria) throws GenieException {
        this.commandCriteria = commandCriteria;
        this.commandCriteriaString = commandCriteriaToString(commandCriteria);
    }

    /**
     * Get the command criteria specified to run this job in string format.
     *
     * @return commandCriteriaString
     */
    public String getCommandCriteriaString() {
        return this.commandCriteriaString;
    }

    /**
     * Set the command criteria string.
     *
     * @param commandCriteriaString A set of command criteria tags
     * @throws GenieException
     */
    public void setCommandCriteriaString(String commandCriteriaString) throws GenieException {
        this.commandCriteriaString = commandCriteriaString;
        this.commandCriteria = stringToCommandCriteria(commandCriteriaString);
    }

    /**
     * Set job status, and update start/update/finish times, if needed.
     *
     * @param jobStatus status for job
     */
    public void setJobStatus(final JobStatus jobStatus) {
        this.status = jobStatus;

        if (jobStatus == JobStatus.INIT) {
            setStartTime(System.currentTimeMillis());
        } else if (jobStatus == JobStatus.SUCCEEDED
                || jobStatus == JobStatus.KILLED
                || jobStatus == JobStatus.FAILED) {
            setFinishTime(System.currentTimeMillis());
        }
    }

    /**
     * Sets job status and human-readable message.
     *
     * @param status predefined status
     * @param msg    human-readable message
     */
    public void setJobStatus(final JobStatus status, final String msg) {
        setJobStatus(status);
        setStatusMsg(msg);
    }

    /**
     * Gets the envPropFile name.
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return this.envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile contains the list of env variables to set while
     *                    running this job.
     */
    public void setEnvPropFile(final String envPropFile) {
        this.envPropFile = envPropFile;
    }

    /**
     * Gets the tags allocated to this job.
     *
     * @return the tags as an unmodifiable list
     */
    public Set<String> getTags() {
        return this.tags;
    }

    /**
     * Sets the tags allocated to this job.
     *
     * @param tags the tags to set. Not Null.
     * @throws GenieException
     */
    public void setTags(final Set<String> tags) throws GenieException {
        this.tags = tags;
    }

    /**
     * Gets the criteria used to select a cluster for this job.
     *
     * @return the criteria containing tags which was chosen to
     * select a cluster to run this job.
     */
    public String getChosenClusterCriteriaString() {
        return chosenClusterCriteriaString;
    }

    /**
     * Sets the criteria used to select cluster to run this job.
     *
     * @param chosenClusterCriteriaString he criteria used to select cluster to run this job.
     * @throws GenieException
     */
    public void setChosenClusterCriteriaString(String chosenClusterCriteriaString) {
        this.chosenClusterCriteriaString = chosenClusterCriteriaString;
    }

    /**
     * Check to make sure that the required parameters exist.
     *
     * @throws GenieException
     */
    @Override
    public void validate() throws GenieException {
        super.validate();
        this.validate(
                this.commandCriteria,
                this.commandArgs,
                this.clusterCriteria);
    }

    /**
     * Validate that required parameters are present for a Job.
     *
     * @param commandCriteria The criteria for the command..
     * @param commandArgs     The command line arguments for the job
     * @param criteria        The cluster criteria for the job
     * @throws GenieException
     */
    private void validate(
            final Set<String> commandCriteria,
            final String commandArgs,
            final List<ClusterCriteria> criteria) throws GenieException {
        final StringBuilder builder = new StringBuilder();
        if (commandCriteria == null || commandCriteria.isEmpty()) {
            builder.append("Command criteria is mandatory to figure out a command to run the job.\n");
        }

        if (StringUtils.isEmpty(commandArgs)) {
            builder.append("Command arguments are required\n");
        }
        if (criteria == null || criteria.isEmpty()) {
            builder.append("At least one cluster criteria is required in order to figure out where to run this job.\n");
        }

        if (builder.length() != 0) {
            builder.insert(0, "Job configuration errors:\n");
            final String msg = builder.toString();
            LOG.error(msg);
            throw new GenieException(HttpURLConnection.HTTP_BAD_REQUEST, msg);
        }
    }

    /**
     * Helper method for building the cluster criteria string.
     *
     * @param clusterCriteria2 The criteria to build up from
     * @return The cluster criteria string
     */
    private String clusterCriteriaToString(final List<ClusterCriteria> clusterCriteria2) throws GenieException {
        if (clusterCriteria2 == null || clusterCriteria2.isEmpty()) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster criteria entered unable to create string");
        }
        final StringBuilder builder = new StringBuilder();
        for (final ClusterCriteria cc : clusterCriteria2) {
            if (builder.length() != 0) {
                builder.append(CRITERIA_SET_DELIMITER);
            }
            builder.append(StringUtils.join(cc.getTags(), CRITERIA_DELIMITER));
        }
        return builder.toString();
    }

    /**
     * Helper method for building the cluster criteria string.
     *
     * @param commandCriteria2 The criteria to build up from
     * @return The cluster criteria string
     */
    private String commandCriteriaToString(final Set<String> commandCriteria2) throws GenieException {
        if (commandCriteria2 == null || commandCriteria2.isEmpty()) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command criteria entered unable to create string");
        }
        return StringUtils.join(commandCriteria, CRITERIA_DELIMITER);
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     * @throws GenieException
     */
    private Set<String> stringToCommandCriteria(final String criteriaString) throws GenieException {
        final String[] criterias = StringUtils.split(criteriaString, CRITERIA_DELIMITER);
        if (criterias == null || criterias.length == 0) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No command criteria found. Unable to continue.");
        }
        final Set<String> c = new HashSet<String>();
        for (final String criteria : criterias) {
            c.add(criteria);
        }
        return c;
    }

    /**
     * Convert a string to cluster criteria objects.
     *
     * @param criteriaString The string to convert
     * @return The set of ClusterCriteria
     * @throws GenieException
     */
    private List<ClusterCriteria> stringToClusterCriteria(final String criteriaString) throws GenieException {
        //Rebuild the cluster criteria objects
        final List<ClusterCriteria> cc = new ArrayList<ClusterCriteria>();
        final String[] criteriaSets = StringUtils.split(criteriaString, CRITERIA_SET_DELIMITER);
        if (criteriaSets == null || criteriaSets.length == 0) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No cluster criteria found. Unable to continue.");
        }
        for (final String criteriaSet : criteriaSets) {
            final String[] criterias = StringUtils.split(criteriaSet, CRITERIA_DELIMITER);
            if (criterias == null || criterias.length == 0) {
                continue;
            }
            final Set<String> c = new HashSet<String>();
            for (final String criteria : criterias) {
                c.add(criteria);
            }
            cc.add(new ClusterCriteria(c));
        }
        if (cc.isEmpty()) {
            throw new GenieException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No Cluster Criteria found. Unable to continue");
        }
        return cc;
    }
}
