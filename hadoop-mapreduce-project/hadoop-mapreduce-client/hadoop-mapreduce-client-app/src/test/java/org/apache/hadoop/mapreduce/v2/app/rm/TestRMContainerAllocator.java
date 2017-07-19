/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app.rm;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.ClusterInfo;
import org.apache.hadoop.mapreduce.v2.app.MRApp;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.JobStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttemptStateInternal;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.job.impl.JobImpl;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ControlledClock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import org.mockito.InOrder;

@SuppressWarnings("unchecked")
public class TestRMContainerAllocator {

  static final Log LOG = LogFactory
      .getLog(TestRMContainerAllocator.class);
  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  @Before
  public void setup() {
    MyContainerAllocator.getJobUpdatedNodeEvents().clear();
    MyContainerAllocator.getTaskAttemptKillEvents().clear();

    // make each test create a fresh user to avoid leaking tokens between tests
    UserGroupInformation.setLoginUser(null);
  }

  @After
  public void tearDown() {
    DefaultMetricsSystem.shutdown();
  }

  @Test
  public void testSimple() throws Exception {

    LOG.info("Running testSimple");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0, 
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    Assert.assertEquals(4, rm.getMyFifoScheduler().lastAsk.size());

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h3" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    Assert.assertEquals(3, rm.getMyFifoScheduler().lastAsk.size());
    
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, rm.getMyFifoScheduler().lastAsk.size());
    checkAssignments(new ContainerRequestEvent[] { event1, event2, event3 },
        assigned, false);
    
    // check that the assigned container requests are cancelled
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(5, rm.getMyFifoScheduler().lastAsk.size());    
  }
  
  @Test 
  public void testMapNodeLocality() throws Exception {
    // test checks that ordering of allocated containers list from the RM does 
    // not affect the map->container assignment done by the AM. If there is a 
    // node local container available for a map then it should be assigned to 
    // that container and not a rack-local container that happened to be seen 
    // earlier in the allocated containers list from the RM.
    // Regression test for MAPREDUCE-4893
    LOG.info("Running testMapNodeLocality");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0, 
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 3072); // can assign 2 maps 
    rm.registerNode("h2:1234", 10240); // wont heartbeat on node local node
    MockNM nodeManager3 = rm.registerNode("h3:1234", 1536); // assign 1 map
    dispatcher.await();

    // create the container requests for maps
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    // Node heartbeat from rack-local first. This makes node h3 the first in the
    // list of allocated containers but it should not be assigned to task1.
    nodeManager3.nodeHeartbeat(true);
    // Node heartbeat from node-local next. This allocates 2 node local 
    // containers for task1 and task2. These should be matched with those tasks.
    nodeManager1.nodeHeartbeat(true);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event2, event3 },
        assigned, false);
    // remove the rack-local assignment that should have happened for task3
    for(TaskAttemptContainerAssignedEvent event : assigned) {
      if(event.getTaskAttemptID().equals(event3.getAttemptID())) {
        assigned.remove(event);
        Assert.assertTrue(
                    event.getContainer().getNodeId().getHost().equals("h3"));
        break;
      }
    }
    checkAssignments(new ContainerRequestEvent[] { event1, event2},
        assigned, true);
  }

  @Test
  public void testResource() throws Exception {

    LOG.info("Running testResource");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 2048,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event2 },
        assigned, false);
  }

  @Test(timeout = 30000)
  public void testReducerRampdownDiagnostics() throws Exception {
    LOG.info("Running tesReducerRampdownDiagnostics");

    final Configuration conf = new Configuration();
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.0f);
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    final DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();
    final RMApp app = rm.submitApp(1024);
    dispatcher.await();

    final String host = "host1";
    final MockNM nm = rm.registerNode(String.format("%s:1234", host), 2048);
    nm.nodeHeartbeat(true);
    dispatcher.await();
    final ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    final JobId jobId = MRBuilderUtils
        .newJobId(appAttemptId.getApplicationId(), 0);
    final Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    final MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob, new SystemClock());
    // add resources to scheduler
    dispatcher.await();

    // create the container request
    final String[] locations = new String[] { host };
    allocator.sendRequest(createReq(jobId, 0, 1024, locations, false, true));
    for (int i = 0; i < 1;) {
      dispatcher.await();
      i += allocator.schedule().size();
      nm.nodeHeartbeat(true);
    }

    allocator.sendRequest(createReq(jobId, 0, 1024, locations, true, false));
    while (allocator.getTaskAttemptKillEvents().size() == 0) {
      dispatcher.await();
      allocator.schedule().size();
      nm.nodeHeartbeat(true);
    }
    final String killEventMessage = allocator.getTaskAttemptKillEvents().get(0)
        .getMessage();
    Assert.assertTrue("No reducer rampDown preemption message",
        killEventMessage.contains(RMContainerAllocator.RAMPDOWN_DIAGNOSTIC));
  }

  @Test(timeout = 30000)
  public void testPreemptReducers() throws Exception {
    LOG.info("Running testPreemptReducers");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob, new SystemClock());
    allocator.setMapResourceRequest(BuilderUtils.newResource(1024, 1));
    allocator.setReduceResourceRequest(BuilderUtils.newResource(1024, 1));
    RMContainerAllocator.AssignedRequests assignedRequests =
        allocator.getAssignedRequests();
    RMContainerAllocator.ScheduledRequests scheduledRequests =
        allocator.getScheduledRequests();
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 2048, new String[] { "h1" }, false, false);
    scheduledRequests.maps.put(mock(TaskAttemptId.class),
        new RMContainerRequestor.ContainerRequest(event1, null));
    assignedRequests.reduces.put(mock(TaskAttemptId.class),
        mock(Container.class));

    allocator.preemptReducesIfNeeded();
    Assert.assertEquals("The reducer is not preempted",
        1, assignedRequests.preemptionWaitingReduces.size());
  }

  @Test(timeout = 30000)
  public void testNonAggressivelyPreemptReducers() throws Exception {
    LOG.info("Running testNonAggressivelyPreemptReducers");

    final int preemptThreshold = 2; //sec
    Configuration conf = new Configuration();
    conf.setInt(
        MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        preemptThreshold);

    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    ControlledClock clock = new ControlledClock(null);
    clock.setTime(1);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob, clock);
    allocator.setMapResourceRequest(BuilderUtils.newResource(1024, 1));
    allocator.setReduceResourceRequest(BuilderUtils.newResource(1024, 1));
    RMContainerAllocator.AssignedRequests assignedRequests =
        allocator.getAssignedRequests();
    RMContainerAllocator.ScheduledRequests scheduledRequests =
        allocator.getScheduledRequests();
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 2048, new String[] { "h1" }, false, false);
    scheduledRequests.maps.put(mock(TaskAttemptId.class),
        new RMContainerRequestor.ContainerRequest(event1, null, clock.getTime()));
    assignedRequests.reduces.put(mock(TaskAttemptId.class),
        mock(Container.class));

    clock.setTime(clock.getTime() + 1);
    allocator.preemptReducesIfNeeded();
    Assert.assertEquals("The reducer is aggressively preeempted", 0,
        assignedRequests.preemptionWaitingReduces.size());

    clock.setTime(clock.getTime() + (preemptThreshold) * 1000);
    allocator.preemptReducesIfNeeded();
    Assert.assertEquals("The reducer is not preeempted", 1,
        assignedRequests.preemptionWaitingReduces.size());
  }

  @Test(timeout = 30000)
  public void testUnconditionalPreemptReducers() throws Exception {
    LOG.info("Running testForcePreemptReducers");

    int forcePreemptThresholdSecs = 2;
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        2 * forcePreemptThresholdSecs);
    conf.setInt(MRJobConfig.MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC,
        forcePreemptThresholdSecs);

    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(8192, 8));
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    ControlledClock clock = new ControlledClock(null);
    clock.setTime(1);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob, clock);
    allocator.setMapResourceRequest(BuilderUtils.newResource(1024, 1));
    allocator.setReduceResourceRequest(BuilderUtils.newResource(1024, 1));
    RMContainerAllocator.AssignedRequests assignedRequests =
        allocator.getAssignedRequests();
    RMContainerAllocator.ScheduledRequests scheduledRequests =
        allocator.getScheduledRequests();
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 2048, new String[] { "h1" }, false, false);
    scheduledRequests.maps.put(mock(TaskAttemptId.class),
        new RMContainerRequestor.ContainerRequest(event1, null, clock.getTime()));
    assignedRequests.reduces.put(mock(TaskAttemptId.class),
        mock(Container.class));

    clock.setTime(clock.getTime() + 1);
    allocator.preemptReducesIfNeeded();
    Assert.assertEquals("The reducer is preeempted too soon", 0,
        assignedRequests.preemptionWaitingReduces.size());

    clock.setTime(clock.getTime() + 1000 * forcePreemptThresholdSecs);
    allocator.preemptReducesIfNeeded();
    Assert.assertEquals("The reducer is not preeempted", 1,
        assignedRequests.preemptionWaitingReduces.size());
  }

  @Test
  public void testMapReduceScheduling() throws Exception {

    LOG.info("Running testMapReduceScheduling");

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob, new SystemClock());

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 1024);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    // send MAP request
    ContainerRequestEvent event1 = createReq(jobId, 1, 2048, new String[] {
        "h1", "h2" }, true, false);
    allocator.sendRequest(event1);

    // send REDUCE request
    ContainerRequestEvent event2 = createReq(jobId, 2, 3000,
        new String[] { "h1" }, false, true);
    allocator.sendRequest(event2);

    // send MAP request
    ContainerRequestEvent event3 = createReq(jobId, 3, 2048,
        new String[] { "h3" }, false, false);
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    checkAssignments(new ContainerRequestEvent[] { event1, event3 },
        assigned, false);

    // validate that no container is assigned to h1 as it doesn't have 2048
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertFalse("Assigned count not correct", "h1".equals(assig
          .getContainer().getNodeId().getHost()));
    }
  }

  private static class MyResourceManager extends MockRM {

    private static long fakeClusterTimeStamp = System.currentTimeMillis();
    
    public MyResourceManager(Configuration conf) {
      super(conf);
    }

    public MyResourceManager(Configuration conf, RMStateStore store) {
      super(conf, store);
    }

    @Override
    public void serviceStart() throws Exception {
      super.serviceStart();
      // Ensure that the application attempt IDs for all the tests are the same
      // The application attempt IDs will be used as the login user names
      MyResourceManager.setClusterTimeStamp(fakeClusterTimeStamp);
    }

    @Override
    protected Dispatcher createDispatcher() {
      return new DrainDispatcher();
    }

    @Override
    protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
      // Dispatch inline for test sanity
      return new EventHandler<SchedulerEvent>() {
        @Override
        public void handle(SchedulerEvent event) {
          scheduler.handle(event);
        }
      };
    }
    @Override
    protected ResourceScheduler createScheduler() {
      return new MyFifoScheduler(this.getRMContext());
    }
    
    MyFifoScheduler getMyFifoScheduler() {
      return (MyFifoScheduler) scheduler;
    }
  }

  @Test
  public void testReportedAppProgress() throws Exception {

    LOG.info("Running testReportedAppProgress");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp rmApp = rm.submitApp(1024);
    rmDispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 21504);
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();

    final ApplicationAttemptId appAttemptId = rmApp.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rmDispatcher.await();

    MRApp mrApp = new MRApp(appAttemptId, ContainerId.newContainerId(
      appAttemptId, 0), 10, 10, false, this.getClass().getName(), true, 1) {
      @Override
      protected Dispatcher createDispatcher() {
        return new DrainDispatcher();
      }
      protected ContainerAllocator createContainerAllocator(
          ClientService clientService, AppContext context) {
        return new MyContainerAllocator(rm, appAttemptId, context);
      };
    };

    Assert.assertEquals(0.0, rmApp.getProgress(), 0.0);

    mrApp.submit(conf);
    Job job = mrApp.getContext().getAllJobs().entrySet().iterator().next()
        .getValue();

    DrainDispatcher amDispatcher = (DrainDispatcher) mrApp.getDispatcher();

    MyContainerAllocator allocator = (MyContainerAllocator) mrApp
      .getContainerAllocator();

    mrApp.waitForInternalState((JobImpl) job, JobStateInternal.RUNNING);

    amDispatcher.await();
    // Wait till all map-attempts request for containers
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.MAP) {
        mrApp.waitForInternalState((TaskAttemptImpl) t.getAttempts().values()
            .iterator().next(), TaskAttemptStateInternal.UNASSIGNED);
      }
    }
    amDispatcher.await();

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all map-tasks to be running
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.MAP) {
        mrApp.waitForState(t, TaskState.RUNNING);
      }
    }

    allocator.schedule(); // Send heartbeat
    rmDispatcher.await();
    Assert.assertEquals(0.05f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.05f, rmApp.getProgress(), 0.001f);

    // Finish off 1 map.
    Iterator<Task> it = job.getTasks().values().iterator();
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.095f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.095f, rmApp.getProgress(), 0.001f);

    // Finish off 7 more so that map-progress is 80%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 7);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.41f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.41f, rmApp.getProgress(), 0.001f);

    // Finish off the 2 remaining maps
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all reduce-tasks to be running
    for (Task t : job.getTasks().values()) {
      if (t.getType() == TaskType.REDUCE) {
        mrApp.waitForState(t, TaskState.RUNNING);
      }
    }

    // Finish off 2 reduces
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 2);

    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.59f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.59f, rmApp.getProgress(), 0.001f);

    // Finish off the remaining 8 reduces.
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 8);
    allocator.schedule();
    rmDispatcher.await();
    // Remaining is JobCleanup
    Assert.assertEquals(0.95f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.95f, rmApp.getProgress(), 0.001f);
  }

  private void finishNextNTasks(DrainDispatcher rmDispatcher, MockNM node,
      MRApp mrApp, Iterator<Task> it, int nextN) throws Exception {
    Task task;
    for (int i=0; i<nextN; i++) {
      task = it.next();
      finishTask(rmDispatcher, node, mrApp, task);
    }
  }

  private void finishTask(DrainDispatcher rmDispatcher, MockNM node,
      MRApp mrApp, Task task) throws Exception {
    TaskAttempt attempt = task.getAttempts().values().iterator().next();
    List<ContainerStatus> contStatus = new ArrayList<ContainerStatus>(1);
    contStatus.add(ContainerStatus.newInstance(attempt.getAssignedContainerID(),
        ContainerState.COMPLETE, "", 0));
    Map<ApplicationId,List<ContainerStatus>> statusUpdate =
        new HashMap<ApplicationId,List<ContainerStatus>>(1);
    statusUpdate.put(mrApp.getAppID(), contStatus);
    node.nodeHeartbeat(statusUpdate, true);
    rmDispatcher.await();
    mrApp.getContext().getEventHandler().handle(
          new TaskAttemptEvent(attempt.getID(), TaskAttemptEventType.TA_DONE));
    mrApp.waitForState(task, TaskState.SUCCEEDED);
  }

  @Test
  public void testReportedAppProgressWithOnlyMaps() throws Exception {

    LOG.info("Running testReportedAppProgressWithOnlyMaps");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp rmApp = rm.submitApp(1024);
    rmDispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 11264);
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();

    final ApplicationAttemptId appAttemptId = rmApp.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rmDispatcher.await();

    MRApp mrApp = new MRApp(appAttemptId, ContainerId.newContainerId(
      appAttemptId, 0), 10, 0, false, this.getClass().getName(), true, 1) {
      @Override
        protected Dispatcher createDispatcher() {
          return new DrainDispatcher();
        }
      protected ContainerAllocator createContainerAllocator(
          ClientService clientService, AppContext context) {
        return new MyContainerAllocator(rm, appAttemptId, context);
      };
    };

    Assert.assertEquals(0.0, rmApp.getProgress(), 0.0);

    mrApp.submit(conf);
    Job job = mrApp.getContext().getAllJobs().entrySet().iterator().next()
        .getValue();

    DrainDispatcher amDispatcher = (DrainDispatcher) mrApp.getDispatcher();

    MyContainerAllocator allocator = (MyContainerAllocator) mrApp
      .getContainerAllocator();

    mrApp.waitForInternalState((JobImpl)job, JobStateInternal.RUNNING);

    amDispatcher.await();
    // Wait till all map-attempts request for containers
    for (Task t : job.getTasks().values()) {
      mrApp.waitForInternalState((TaskAttemptImpl) t.getAttempts().values()
          .iterator().next(), TaskAttemptStateInternal.UNASSIGNED);
    }
    amDispatcher.await();

    allocator.schedule();
    rmDispatcher.await();
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();
    allocator.schedule();
    rmDispatcher.await();

    // Wait for all map-tasks to be running
    for (Task t : job.getTasks().values()) {
      mrApp.waitForState(t, TaskState.RUNNING);
    }

    allocator.schedule(); // Send heartbeat
    rmDispatcher.await();
    Assert.assertEquals(0.05f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.05f, rmApp.getProgress(), 0.001f);

    Iterator<Task> it = job.getTasks().values().iterator();

    // Finish off 1 map so that map-progress is 10%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 1);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.14f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.14f, rmApp.getProgress(), 0.001f);

    // Finish off 5 more map so that map-progress is 60%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 5);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.59f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.59f, rmApp.getProgress(), 0.001f);

    // Finish off remaining map so that map-progress is 100%
    finishNextNTasks(rmDispatcher, amNodeManager, mrApp, it, 4);
    allocator.schedule();
    rmDispatcher.await();
    Assert.assertEquals(0.95f, job.getProgress(), 0.001f);
    Assert.assertEquals(0.95f, rmApp.getProgress(), 0.001f);
  }
  
  @Test
  public void testUpdatedNodes() throws Exception {
    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nm1 = rm.registerNode("h1:1234", 10240);
    MockNM nm2 = rm.registerNode("h2:1234", 10240);
    dispatcher.await();

    // create the map container request
    ContainerRequestEvent event = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event);
    TaskAttemptId attemptId = event.getAttemptID();
    
    TaskAttempt mockTaskAttempt = mock(TaskAttempt.class);
    when(mockTaskAttempt.getNodeId()).thenReturn(nm1.getNodeId());
    Task mockTask = mock(Task.class);
    when(mockTask.getAttempt(attemptId)).thenReturn(mockTaskAttempt);
    when(mockJob.getTask(attemptId.getTaskId())).thenReturn(mockTask);

    // this tells the scheduler about the requests
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();

    nm1.nodeHeartbeat(true);
    dispatcher.await();
    Assert.assertEquals(1, allocator.getJobUpdatedNodeEvents().size());
    Assert.assertEquals(3, allocator.getJobUpdatedNodeEvents().get(0).getUpdatedNodes().size());
    allocator.getJobUpdatedNodeEvents().clear();
    // get the assignment
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(1, assigned.size());
    Assert.assertEquals(nm1.getNodeId(), assigned.get(0).getContainer().getNodeId());
    // no updated nodes reported
    Assert.assertTrue(allocator.getJobUpdatedNodeEvents().isEmpty());
    Assert.assertTrue(allocator.getTaskAttemptKillEvents().isEmpty());
    
    // mark nodes bad
    nm1.nodeHeartbeat(false);
    nm2.nodeHeartbeat(false);
    dispatcher.await();
    
    // schedule response returns updated nodes
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, assigned.size());
    // updated nodes are reported
    Assert.assertEquals(1, allocator.getJobUpdatedNodeEvents().size());
    Assert.assertEquals(1, allocator.getTaskAttemptKillEvents().size());
    Assert.assertEquals(2, allocator.getJobUpdatedNodeEvents().get(0).getUpdatedNodes().size());
    Assert.assertEquals(attemptId, allocator.getTaskAttemptKillEvents().get(0).getTaskAttemptID());
    allocator.getJobUpdatedNodeEvents().clear();
    allocator.getTaskAttemptKillEvents().clear();
    
    assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals(0, assigned.size());
    // no updated nodes reported
    Assert.assertTrue(allocator.getJobUpdatedNodeEvents().isEmpty());
    Assert.assertTrue(allocator.getTaskAttemptKillEvents().isEmpty());
  }

  @Test
  public void testBlackListedNodes() throws Exception {
    
    LOG.info("Running testBlackListedNodes");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);
    
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager2 = rm.registerNode("h2:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    // send 1 more request with different resource req
    ContainerRequestEvent event2 = createReq(jobId, 2, 1024,
        new String[] { "h2" });
    allocator.sendRequest(event2);

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h3" });
    allocator.sendRequest(event3);

    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);            
    allocator.sendFailure(f1);
    ContainerFailedEvent f2 = createFailEvent(jobId, 1, "h2", false);            
    allocator.sendFailure(f2);

    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    nodeManager2.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    assigned = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    assertBlacklistAdditionsAndRemovals(2, 0, rm);

    // mark h1/h2 as bad nodes
    nodeManager1.nodeHeartbeat(false);
    nodeManager2.nodeHeartbeat(false);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());    

    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
        
    Assert.assertTrue("No of assignments must be 3", assigned.size() == 3);
    
    // validate that all containers are assigned to h3
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertTrue("Assigned container host not correct", "h3".equals(assig
          .getContainer().getNodeId().getHost()));
    }
  }
  
  @Test
  public void testIgnoreBlacklisting() throws Exception {
    LOG.info("Running testIgnoreBlacklisting");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, 33);

    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM[] nodeManagers = new MockNM[10];
    int nmNum = 0;
    List<TaskAttemptContainerAssignedEvent> assigned = null;
    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    nodeManagers[0].nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator =
        new MyContainerAllocator(rm, conf, appAttemptId, mockJob);

    // Known=1, blacklisted=0, ignore should be false - assign first container
    assigned =
        getContainerOnHost(jobId, 1, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    LOG.info("Failing container _1 on H1 (Node should be blacklisted and"
        + " ignore blacklisting enabled");
    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);
    allocator.sendFailure(f1);

    // Test single node.
    // Known=1, blacklisted=1, ignore should be true - assign 0
    // Because makeRemoteRequest will not be aware of it until next call
    // The current call will send blacklisted node "h1" to RM
    assigned =
        getContainerOnHost(jobId, 2, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 1, 0, 0, 1, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Known=1, blacklisted=1, ignore should be true - assign 1
    assigned =
        getContainerOnHost(jobId, 2, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=2, blacklisted=1, ignore should be true - assign 1 anyway.
    assigned =
        getContainerOnHost(jobId, 3, 1024, new String[] { "h2" },
            nodeManagers[1], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=3, blacklisted=1, ignore should be true - assign 1 anyway.
    assigned =
        getContainerOnHost(jobId, 4, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Known=3, blacklisted=1, ignore should be true - assign 1
    assigned =
        getContainerOnHost(jobId, 5, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=4, blacklisted=1, ignore should be false - assign 1 anyway
    assigned =
        getContainerOnHost(jobId, 6, 1024, new String[] { "h4" },
            nodeManagers[3], dispatcher, allocator, 0, 0, 1, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Test blacklisting re-enabled.
    // Known=4, blacklisted=1, ignore should be false - no assignment on h1
    assigned =
        getContainerOnHost(jobId, 7, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    // RMContainerRequestor would have created a replacement request.

    // Blacklist h2
    ContainerFailedEvent f2 = createFailEvent(jobId, 3, "h2", false);
    allocator.sendFailure(f2);

    // Test ignore blacklisting re-enabled
    // Known=4, blacklisted=2, ignore should be true. Should assign 0
    // container for the same reason above.
    assigned =
        getContainerOnHost(jobId, 8, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 1, 0, 0, 2, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Known=4, blacklisted=2, ignore should be true. Should assign 2
    // containers.
    assigned =
        getContainerOnHost(jobId, 8, 1024, new String[] { "h1" },
            nodeManagers[0], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 2", 2, assigned.size());

    // Known=4, blacklisted=2, ignore should be true.
    assigned =
        getContainerOnHost(jobId, 9, 1024, new String[] { "h2" },
            nodeManagers[1], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());

    // Test blacklist while ignore blacklisting enabled
    ContainerFailedEvent f3 = createFailEvent(jobId, 4, "h3", false);
    allocator.sendFailure(f3);

    nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
    // Known=5, blacklisted=3, ignore should be true.
    assigned =
        getContainerOnHost(jobId, 10, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());
    
    // Assign on 5 more nodes - to re-enable blacklisting
    for (int i = 0; i < 5; i++) {
      nodeManagers[nmNum] = registerNodeManager(nmNum++, rm, dispatcher);
      assigned =
          getContainerOnHost(jobId, 11 + i, 1024,
              new String[] { String.valueOf(5 + i) }, nodeManagers[4 + i],
              dispatcher, allocator, 0, 0, (i == 4 ? 3 : 0), 0, rm);
      Assert.assertEquals("No of assignments must be 1", 1, assigned.size());
    }

    // Test h3 (blacklisted while ignoring blacklisting) is blacklisted.
    assigned =
        getContainerOnHost(jobId, 20, 1024, new String[] { "h3" },
            nodeManagers[2], dispatcher, allocator, 0, 0, 0, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
  }

  private MockNM registerNodeManager(int i, MyResourceManager rm,
      DrainDispatcher dispatcher) throws Exception {
    MockNM nm = rm.registerNode("h" + (i + 1) + ":1234", 10240);
    dispatcher.await();
    return nm;
  }

  private
      List<TaskAttemptContainerAssignedEvent> getContainerOnHost(JobId jobId,
          int taskAttemptId, int memory, String[] hosts, MockNM mockNM,
          DrainDispatcher dispatcher, MyContainerAllocator allocator,
          int expectedAdditions1, int expectedRemovals1,
          int expectedAdditions2, int expectedRemovals2, MyResourceManager rm)
          throws Exception {
    ContainerRequestEvent reqEvent =
        createReq(jobId, taskAttemptId, memory, hosts);
    allocator.sendRequest(reqEvent);

    // Send the request to the RM
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(
        expectedAdditions1, expectedRemovals1, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // Heartbeat from the required nodeManager
    mockNM.nodeHeartbeat(true);
    dispatcher.await();

    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(
        expectedAdditions2, expectedRemovals2, rm);
    return assigned;
  }
 
  @Test
  public void testBlackListedNodesWithSchedulingToThatNode() throws Exception {
    LOG.info("Running testBlackListedNodesWithSchedulingToThatNode");

    Configuration conf = new Configuration();
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);
    
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();
    
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // add resources to scheduler
    MockNM nodeManager1 = rm.registerNode("h1:1234", 10240);
    MockNM nodeManager3 = rm.registerNode("h3:1234", 10240);
    dispatcher.await();

    LOG.info("Requesting 1 Containers _1 on H1");
    // create the container request
    ContainerRequestEvent event1 = createReq(jobId, 1, 1024,
        new String[] { "h1" });
    allocator.sendRequest(event1);

    LOG.info("RM Heartbeat (to send the container requests)");
    // this tells the scheduler about the requests
    // as nodes are not added, no allocations
    List<TaskAttemptContainerAssignedEvent> assigned = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    LOG.info("h1 Heartbeat (To actually schedule the containers)");
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
    Assert.assertEquals("No of assignments must be 1", 1, assigned.size());    
    
    LOG.info("Failing container _1 on H1 (should blacklist the node)");
    // Send events to blacklist nodes h1 and h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h1", false);
    allocator.sendFailure(f1);

    //At this stage, a request should be created for a fast fail map
    //Create a FAST_FAIL request for a previously failed map.
    ContainerRequestEvent event1f = createReq(jobId, 1, 1024,
        new String[] { "h1" }, true, false);
    allocator.sendRequest(event1f);

    //Update the Scheduler with the new requests.
    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(1, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());

    // send another request with different resource and priority
    ContainerRequestEvent event3 = createReq(jobId, 3, 1024,
        new String[] { "h1", "h3" });
    allocator.sendRequest(event3);
    
    //Allocator is aware of prio:5 container, and prio:20 (h1+h3) container.
    //RM is only aware of the prio:5 container
    
    LOG.info("h1 Heartbeat (To actually schedule the containers)");
    // update resources in scheduler
    nodeManager1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());    
    
    //RMContainerAllocator gets assigned a p:5 on a blacklisted node.

    //Send a release for the p:5 container + another request.
    LOG.info("RM Heartbeat (To process the re-scheduled containers)");
    assigned = allocator.schedule();
    dispatcher.await();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
    Assert.assertEquals("No of assignments must be 0", 0, assigned.size());
    
    //Hearbeat from H3 to schedule on this host.
    LOG.info("h3 Heartbeat (To re-schedule the containers)");
    nodeManager3.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();
    
    LOG.info("RM Heartbeat (To process the re-scheduled containers for H3)");
    assigned = allocator.schedule();
    assertBlacklistAdditionsAndRemovals(0, 0, rm);
    dispatcher.await();
     
    // For debugging
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      LOG.info(assig.getTaskAttemptID() +
          " assgined to " + assig.getContainer().getId() +
          " with priority " + assig.getContainer().getPriority());
    }
    
    Assert.assertEquals("No of assignments must be 2", 2, assigned.size());
    
    // validate that all containers are assigned to h3
    for (TaskAttemptContainerAssignedEvent assig : assigned) {
      Assert.assertEquals("Assigned container " + assig.getContainer().getId()
          + " host not correct", "h3", assig.getContainer().getNodeId().getHost());
    }
  }

  private static void assertBlacklistAdditionsAndRemovals(
      int expectedAdditions, int expectedRemovals, MyResourceManager rm) {
    Assert.assertEquals(expectedAdditions,
        rm.getMyFifoScheduler().lastBlacklistAdditions.size());
    Assert.assertEquals(expectedRemovals,
        rm.getMyFifoScheduler().lastBlacklistRemovals.size());
  }

  private static void assertAsksAndReleases(int expectedAsk,
      int expectedRelease, MyResourceManager rm) {
    Assert.assertEquals(expectedAsk, rm.getMyFifoScheduler().lastAsk.size());
    Assert.assertEquals(expectedRelease,
        rm.getMyFifoScheduler().lastRelease.size());
  }

  private static class MyFifoScheduler extends FifoScheduler {

    public MyFifoScheduler(RMContext rmContext) {
      super();
      try {
        Configuration conf = new Configuration();
        reinitialize(conf, rmContext);
      } catch (IOException ie) {
        LOG.info("add application failed with ", ie);
        assert (false);
      }
    }
    
    List<ResourceRequest> lastAsk = null;
    List<ContainerId> lastRelease = null;
    List<String> lastBlacklistAdditions;
    List<String> lastBlacklistRemovals;
    Resource forceResourceLimit = null;
    
    // override this to copy the objects otherwise FifoScheduler updates the
    // numContainers in same objects as kept by RMContainerAllocator
    @Override
    public synchronized Allocation allocate(
        ApplicationAttemptId applicationAttemptId, List<ResourceRequest> ask,
        List<ContainerId> release, 
        List<String> blacklistAdditions, List<String> blacklistRemovals) {
      List<ResourceRequest> askCopy = new ArrayList<ResourceRequest>();
      for (ResourceRequest req : ask) {
        ResourceRequest reqCopy = ResourceRequest.newInstance(req
            .getPriority(), req.getResourceName(), req.getCapability(), req
            .getNumContainers(), req.getRelaxLocality());
        askCopy.add(reqCopy);
      }
      SecurityUtil.setTokenServiceUseIp(false);
      lastAsk = ask;
      lastRelease = release;
      lastBlacklistAdditions = blacklistAdditions;
      lastBlacklistRemovals = blacklistRemovals;
      Allocation allocation = super.allocate(
          applicationAttemptId, askCopy, release, blacklistAdditions,
          blacklistRemovals);
      if (forceResourceLimit != null) {
        // Test wants to force the non-default resource limit
        allocation.setResourceLimit(forceResourceLimit);
      }
      return allocation;
    }

    public void forceResourceLimit(Resource resource) {
      this.forceResourceLimit = resource;
    }
  }

  private ContainerRequestEvent createReq(JobId jobId, int taskAttemptId,
      int memory, String[] hosts) {
    return createReq(jobId, taskAttemptId, memory, 1, hosts, false, false);
  }

  private ContainerRequestEvent createReq(JobId jobId, int taskAttemptId,
      int mem, String[] hosts, boolean earlierFailedAttempt, boolean reduce) {
    return createReq(jobId, taskAttemptId, mem,
        1, hosts, earlierFailedAttempt, reduce);
  }

  private ContainerRequestEvent createReq(JobId jobId, int taskAttemptId,
      int memory, int vcore, String[] hosts, boolean earlierFailedAttempt,
      boolean reduce) {
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
        taskAttemptId);
    Resource containerNeed = Resource.newInstance(memory, vcore);
    if (earlierFailedAttempt) {
      return ContainerRequestEvent
          .createContainerRequestEventForFailedContainer(attemptId,
              containerNeed);
    }
    return new ContainerRequestEvent(attemptId, containerNeed, hosts,
        new String[] { NetworkTopology.DEFAULT_RACK });
  }

  private ContainerFailedEvent createFailEvent(JobId jobId, int taskAttemptId,
      String host, boolean reduce) {
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(taskId,
        taskAttemptId);
    return new ContainerFailedEvent(attemptId, host);    
  }
  
  private ContainerAllocatorEvent createDeallocateEvent(JobId jobId,
      int taskAttemptId, boolean reduce) {
    TaskId taskId;
    if (reduce) {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.REDUCE);
    } else {
      taskId = MRBuilderUtils.newTaskId(jobId, 0, TaskType.MAP);
    }
    TaskAttemptId attemptId =
        MRBuilderUtils.newTaskAttemptId(taskId, taskAttemptId);
    return new ContainerAllocatorEvent(attemptId,
        ContainerAllocator.EventType.CONTAINER_DEALLOCATE);
  }

  private void checkAssignments(ContainerRequestEvent[] requests,
      List<TaskAttemptContainerAssignedEvent> assignments,
      boolean checkHostMatch) {
    Assert.assertNotNull("Container not assigned", assignments);
    Assert.assertEquals("Assigned count not correct", requests.length,
        assignments.size());

    // check for uniqueness of containerIDs
    Set<ContainerId> containerIds = new HashSet<ContainerId>();
    for (TaskAttemptContainerAssignedEvent assigned : assignments) {
      containerIds.add(assigned.getContainer().getId());
    }
    Assert.assertEquals("Assigned containers must be different", assignments
        .size(), containerIds.size());

    // check for all assignment
    for (ContainerRequestEvent req : requests) {
      TaskAttemptContainerAssignedEvent assigned = null;
      for (TaskAttemptContainerAssignedEvent ass : assignments) {
        if (ass.getTaskAttemptID().equals(req.getAttemptID())) {
          assigned = ass;
          break;
        }
      }
      checkAssignment(req, assigned, checkHostMatch);
    }
  }

  private void checkAssignment(ContainerRequestEvent request,
      TaskAttemptContainerAssignedEvent assigned, boolean checkHostMatch) {
    Assert.assertNotNull("Nothing assigned to attempt "
        + request.getAttemptID(), assigned);
    Assert.assertEquals("assigned to wrong attempt", request.getAttemptID(),
        assigned.getTaskAttemptID());
    if (checkHostMatch) {
      Assert.assertTrue("Not assigned to requested host", Arrays.asList(
          request.getHosts()).contains(
          assigned.getContainer().getNodeId().getHost()));
    }
  }

  // Mock RMContainerAllocator
  // Instead of talking to remote Scheduler,uses the local Scheduler
  private static class MyContainerAllocator extends RMContainerAllocator {
    static final List<TaskAttemptContainerAssignedEvent> events
      = new ArrayList<TaskAttemptContainerAssignedEvent>();
    static final List<TaskAttemptKillEvent> taskAttemptKillEvents 
      = new ArrayList<TaskAttemptKillEvent>();
    static final List<JobUpdatedNodesEvent> jobUpdatedNodeEvents 
    = new ArrayList<JobUpdatedNodesEvent>();
    static final List<JobEvent> jobEvents = new ArrayList<JobEvent>();
    private MyResourceManager rm;
    private boolean isUnregistered = false;
    private AllocateResponse allocateResponse;
    private static AppContext createAppContext(
        ApplicationAttemptId appAttemptId, Job job) {
      AppContext context = mock(AppContext.class);
      ApplicationId appId = appAttemptId.getApplicationId();
      when(context.getApplicationID()).thenReturn(appId);
      when(context.getApplicationAttemptId()).thenReturn(appAttemptId);
      when(context.getJob(isA(JobId.class))).thenReturn(job);
      when(context.getClock()).thenReturn(
        new ControlledClock(new SystemClock()));
      when(context.getClusterInfo()).thenReturn(
        new ClusterInfo(Resource.newInstance(10240, 1)));
      when(context.getEventHandler()).thenReturn(new EventHandler() {
        @Override
        public void handle(Event event) {
          // Only capture interesting events.
          if (event instanceof TaskAttemptContainerAssignedEvent) {
            events.add((TaskAttemptContainerAssignedEvent) event);
          } else if (event instanceof TaskAttemptKillEvent) {
            taskAttemptKillEvents.add((TaskAttemptKillEvent)event);
          } else if (event instanceof JobUpdatedNodesEvent) {
            jobUpdatedNodeEvents.add((JobUpdatedNodesEvent)event);
          } else if (event instanceof JobEvent) {
            jobEvents.add((JobEvent)event);
          }
        }
      });
      return context;
    }

    private static AppContext createAppContext(
        ApplicationAttemptId appAttemptId, Job job, Clock clock) {
      AppContext context = createAppContext(appAttemptId, job);
      when(context.getClock()).thenReturn(clock);
      return context;
    }

    private static ClientService createMockClientService() {
      ClientService service = mock(ClientService.class);
      when(service.getBindAddress()).thenReturn(
          NetUtils.createSocketAddr("localhost:4567"));
      when(service.getHttpPort()).thenReturn(890);
      return service;
    }

    // Use this constructor when using a real job.
    MyContainerAllocator(MyResourceManager rm,
        ApplicationAttemptId appAttemptId, AppContext context) {
      super(createMockClientService(), context);
      this.rm = rm;
    }

    // Use this constructor when you are using a mocked job.
    public MyContainerAllocator(MyResourceManager rm, Configuration conf,
        ApplicationAttemptId appAttemptId, Job job) {
      super(createMockClientService(), createAppContext(appAttemptId, job));
      this.rm = rm;
      super.init(conf);
      super.start();
    }

    public MyContainerAllocator(MyResourceManager rm, Configuration conf,
        ApplicationAttemptId appAttemptId, Job job, Clock clock) {
      super(createMockClientService(),
          createAppContext(appAttemptId, job, clock));
      this.rm = rm;
      super.init(conf);
      super.start();
    }

    @Override
    protected ApplicationMasterProtocol createSchedulerProxy() {
      return this.rm.getApplicationMasterService();
    }

    @Override
    protected void register() {
      ApplicationAttemptId attemptId = getContext().getApplicationAttemptId();
      Token<AMRMTokenIdentifier> token =
          rm.getRMContext().getRMApps().get(attemptId.getApplicationId())
            .getRMAppAttempt(attemptId).getAMRMToken();
      try {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ugi.addTokenIdentifier(token.decodeIdentifier());
      } catch (IOException e) {
        throw new YarnRuntimeException(e);
      }
      super.register();
    }

    @Override
    protected void unregister() {
      isUnregistered=true;
    }

    @Override
    protected Resource getMaxContainerCapability() {
      return Resource.newInstance(10240, 1);
    }

    public void sendRequest(ContainerRequestEvent req) {
      sendRequests(Arrays.asList(new ContainerRequestEvent[] { req }));
    }

    public void sendRequests(List<ContainerRequestEvent> reqs) {
      for (ContainerRequestEvent req : reqs) {
        super.handleEvent(req);
      }
    }

    public void sendFailure(ContainerFailedEvent f) {
      super.handleEvent(f);
    }
    
    public void sendDeallocate(ContainerAllocatorEvent f) {
      super.handleEvent(f);
    }

    // API to be used by tests
    public List<TaskAttemptContainerAssignedEvent> schedule()
        throws Exception {
      // before doing heartbeat with RM, drain all the outstanding events to
      // ensure all the requests before this heartbeat is to be handled
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        public Boolean get() {
          return eventQueue.isEmpty();
        }
      }, 100, 10000);
      // run the scheduler
      super.heartbeat();

      List<TaskAttemptContainerAssignedEvent> result
        = new ArrayList<TaskAttemptContainerAssignedEvent>(events);
      events.clear();
      return result;
    }
    
    static List<TaskAttemptKillEvent> getTaskAttemptKillEvents() {
      return taskAttemptKillEvents;
    }
    
    static List<JobUpdatedNodesEvent> getJobUpdatedNodeEvents() {
      return jobUpdatedNodeEvents;
    }

    @Override
    protected void startAllocatorThread() {
      // override to NOT start thread
    }
    
    @Override
    protected boolean isApplicationMasterRegistered() {
      return super.isApplicationMasterRegistered();
    }
    
    public boolean isUnregistered() {
      return isUnregistered;
    }

    public void updateSchedulerProxy(MyResourceManager rm) {
      scheduler = rm.getApplicationMasterService();
    }

    @Override
    protected AllocateResponse makeRemoteRequest() throws IOException,
        YarnException {
      allocateResponse = super.makeRemoteRequest();
      return allocateResponse;
    }
  }

  private static class MyContainerAllocator2 extends MyContainerAllocator {
    public MyContainerAllocator2(MyResourceManager rm, Configuration conf,
      ApplicationAttemptId appAttemptId, Job job) {
      super(rm, conf, appAttemptId, job);
    }
    @Override
    protected AllocateResponse makeRemoteRequest() throws IOException,
      YarnException {
      throw new IOException("for testing");
    }
  }

  @Test
  public void testReduceScheduling() throws Exception {
    int totalMaps = 10;
    int succeededMaps = 1;
    int scheduledMaps = 10;
    int scheduledReduces = 0;
    int assignedMaps = 2;
    int assignedReduces = 0;
    Resource mapResourceReqt = BuilderUtils.newResource(1024, 1);
    Resource reduceResourceReqt = BuilderUtils.newResource(2 * 1024, 1);
    int numPendingReduces = 4;
    float maxReduceRampupLimit = 0.5f;
    float reduceSlowStart = 0.2f;
    
    RMContainerAllocator allocator = mock(RMContainerAllocator.class);
    doCallRealMethod().when(allocator).scheduleReduces(anyInt(), anyInt(),
        anyInt(), anyInt(), anyInt(), anyInt(), any(Resource.class),
        any(Resource.class), anyInt(), anyFloat(), anyFloat());
    doReturn(EnumSet.of(SchedulerResourceTypes.MEMORY)).when(allocator)
      .getSchedulerResourceTypes();

    // Test slow-start
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, never()).setIsReduceStarted(true);
    
    // verify slow-start still in effect when no more maps need to
    // be scheduled but some have yet to complete
    allocator.scheduleReduces(
        totalMaps, succeededMaps,
        0, scheduledReduces,
        totalMaps - succeededMaps, assignedReduces,
        mapResourceReqt, reduceResourceReqt,
        numPendingReduces,
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, never()).setIsReduceStarted(true);
    verify(allocator, never()).scheduleAllReduces();

    succeededMaps = 3;
    doReturn(BuilderUtils.newResource(0, 0)).when(allocator).getResourceLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, times(1)).setIsReduceStarted(true);
    
    // Test reduce ramp-up
    doReturn(BuilderUtils.newResource(100 * 1024, 100 * 1)).when(allocator)
      .getResourceLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator).rampUpReduces(anyInt());
    verify(allocator, never()).rampDownReduces(anyInt());

    // Test reduce ramp-down
    scheduledReduces = 3;
    doReturn(BuilderUtils.newResource(10 * 1024, 10 * 1)).when(allocator)
      .getResourceLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps,
        scheduledMaps, scheduledReduces,
        assignedMaps, assignedReduces,
        mapResourceReqt, reduceResourceReqt,
        numPendingReduces,
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator).rampDownReduces(anyInt());

    // Test reduce ramp-down for when there are scheduled maps
    // Since we have two scheduled Maps, rampDownReducers 
    // should be invoked twice.
    scheduledMaps = 2;
    assignedReduces = 2;
    doReturn(BuilderUtils.newResource(10 * 1024, 10 * 1)).when(allocator)
      .getResourceLimit();
    allocator.scheduleReduces(
        totalMaps, succeededMaps, 
        scheduledMaps, scheduledReduces, 
        assignedMaps, assignedReduces, 
        mapResourceReqt, reduceResourceReqt, 
        numPendingReduces, 
        maxReduceRampupLimit, reduceSlowStart);
    verify(allocator, times(2)).rampDownReduces(anyInt());

    doReturn(
        EnumSet.of(SchedulerResourceTypes.MEMORY, SchedulerResourceTypes.CPU))
        .when(allocator).getSchedulerResourceTypes();

    // Test ramp-down when enough memory but not enough cpu resource
    scheduledMaps = 10;
    assignedReduces = 0;
    doReturn(BuilderUtils.newResource(100 * 1024, 5 * 1)).when(allocator)
        .getResourceLimit();
    allocator.scheduleReduces(totalMaps, succeededMaps, scheduledMaps,
        scheduledReduces, assignedMaps, assignedReduces, mapResourceReqt,
        reduceResourceReqt, numPendingReduces, maxReduceRampupLimit,
        reduceSlowStart);
    verify(allocator, times(3)).rampDownReduces(anyInt());

    // Test ramp-down when enough cpu but not enough memory resource
    doReturn(BuilderUtils.newResource(10 * 1024, 100 * 1)).when(allocator)
        .getResourceLimit();
    allocator.scheduleReduces(totalMaps, succeededMaps, scheduledMaps,
        scheduledReduces, assignedMaps, assignedReduces, mapResourceReqt,
        reduceResourceReqt, numPendingReduces, maxReduceRampupLimit,
        reduceSlowStart);
    verify(allocator, times(4)).rampDownReduces(anyInt());
  }

  private static class RecalculateContainerAllocator extends MyContainerAllocator {
    public boolean recalculatedReduceSchedule = false;

    public RecalculateContainerAllocator(MyResourceManager rm,
        Configuration conf, ApplicationAttemptId appAttemptId, Job job) {
      super(rm, conf, appAttemptId, job);
    }

    @Override
    public void scheduleReduces(int totalMaps, int completedMaps,
        int scheduledMaps, int scheduledReduces, int assignedMaps,
        int assignedReduces, Resource mapResourceReqt, Resource reduceResourceReqt,
        int numPendingReduces, float maxReduceRampupLimit, float reduceSlowStart) {
      recalculatedReduceSchedule = true;
    }
  }
  
  @Test
  public void testCompletedTasksRecalculateSchedule() throws Exception {
    LOG.info("Running testCompletedTasksRecalculateSchedule");

    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    // Make a node to register so as to launch the AM.
    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job job = mock(Job.class);
    when(job.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    doReturn(10).when(job).getTotalMaps();
    doReturn(10).when(job).getTotalReduces();
    doReturn(0).when(job).getCompletedMaps();
    RecalculateContainerAllocator allocator =
        new RecalculateContainerAllocator(rm, conf, appAttemptId, job);
    allocator.schedule();

    allocator.recalculatedReduceSchedule = false;
    allocator.schedule();
    Assert.assertFalse("Unexpected recalculate of reduce schedule",
        allocator.recalculatedReduceSchedule);

    doReturn(1).when(job).getCompletedMaps();
    allocator.schedule();
    Assert.assertTrue("Expected recalculate of reduce schedule",
        allocator.recalculatedReduceSchedule);
  }

  @Test
  public void testHeartbeatHandler() throws Exception {
    LOG.info("Running testHeartbeatHandler");

    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_AM_TO_RM_HEARTBEAT_INTERVAL_MS, 1);
    ControlledClock clock = new ControlledClock(new SystemClock());
    AppContext appContext = mock(AppContext.class);
    when(appContext.getClock()).thenReturn(clock);
    when(appContext.getApplicationID()).thenReturn(
        ApplicationId.newInstance(1, 1));

    RMContainerAllocator allocator = new RMContainerAllocator(
        mock(ClientService.class), appContext) {
          @Override
          protected void register() {
          }
          @Override
          protected ApplicationMasterProtocol createSchedulerProxy() {
            return mock(ApplicationMasterProtocol.class);
          }
          @Override
          protected synchronized void heartbeat() throws Exception {
          }
    };
    allocator.init(conf);
    allocator.start();

    clock.setTime(5);
    int timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 5 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(5, allocator.getLastHeartbeatTime());
    clock.setTime(7);
    timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 7 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(7, allocator.getLastHeartbeatTime());

    final AtomicBoolean callbackCalled = new AtomicBoolean(false);
    allocator.runOnNextHeartbeat(new Runnable() {
      @Override
      public void run() {
        callbackCalled.set(true);
      }
    });
    clock.setTime(8);
    timeToWaitMs = 5000;
    while (allocator.getLastHeartbeatTime() != 8 && timeToWaitMs > 0) {
      Thread.sleep(10);
      timeToWaitMs -= 10;
    }
    Assert.assertEquals(8, allocator.getLastHeartbeatTime());
    Assert.assertTrue(callbackCalled.get());
  }

  @Test
  public void testCompletedContainerEvent() {
    RMContainerAllocator allocator = new RMContainerAllocator(
        mock(ClientService.class), mock(AppContext.class));
    
    TaskAttemptId attemptId = MRBuilderUtils.newTaskAttemptId(
        MRBuilderUtils.newTaskId(
            MRBuilderUtils.newJobId(1, 1, 1), 1, TaskType.MAP), 1);
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(
        applicationId, 1);
    ContainerId containerId = ContainerId.newContainerId(applicationAttemptId, 1);
    ContainerStatus status = ContainerStatus.newInstance(
        containerId, ContainerState.RUNNING, "", 0);

    ContainerStatus abortedStatus = ContainerStatus.newInstance(
        containerId, ContainerState.RUNNING, "",
        ContainerExitStatus.ABORTED);
    
    TaskAttemptEvent event = allocator.createContainerFinishedEvent(status,
        attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_CONTAINER_COMPLETED,
        event.getType());
    
    TaskAttemptEvent abortedEvent = allocator.createContainerFinishedEvent(
        abortedStatus, attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_KILL, abortedEvent.getType());
    
    ContainerId containerId2 = ContainerId.newContainerId(applicationAttemptId, 2);
    ContainerStatus status2 = ContainerStatus.newInstance(containerId2,
        ContainerState.RUNNING, "", 0);

    ContainerStatus preemptedStatus = ContainerStatus.newInstance(containerId2,
        ContainerState.RUNNING, "", ContainerExitStatus.PREEMPTED);

    TaskAttemptEvent event2 = allocator.createContainerFinishedEvent(status2,
        attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_CONTAINER_COMPLETED,
        event2.getType());

    TaskAttemptEvent abortedEvent2 = allocator.createContainerFinishedEvent(
        preemptedStatus, attemptId);
    Assert.assertEquals(TaskAttemptEventType.TA_KILL, abortedEvent2.getType());
  }
  
  @Test
  public void testUnregistrationOnlyIfRegistered() throws Exception {
    Configuration conf = new Configuration();
    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher rmDispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp rmApp = rm.submitApp(1024);
    rmDispatcher.await();

    MockNM amNodeManager = rm.registerNode("127.0.0.1:1234", 11264);
    amNodeManager.nodeHeartbeat(true);
    rmDispatcher.await();

    final ApplicationAttemptId appAttemptId =
        rmApp.getCurrentAppAttempt().getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    rmDispatcher.await();

    MRApp mrApp =
        new MRApp(appAttemptId, ContainerId.newContainerId(appAttemptId, 0), 10,
            0, false, this.getClass().getName(), true, 1) {
          @Override
          protected Dispatcher createDispatcher() {
            return new DrainDispatcher();
          }

          protected ContainerAllocator createContainerAllocator(
              ClientService clientService, AppContext context) {
            return new MyContainerAllocator(rm, appAttemptId, context);
          };
        };

    mrApp.submit(conf);
    DrainDispatcher amDispatcher = (DrainDispatcher) mrApp.getDispatcher();
    MyContainerAllocator allocator =
        (MyContainerAllocator) mrApp.getContainerAllocator();
    amDispatcher.await();
    Assert.assertTrue(allocator.isApplicationMasterRegistered());
    mrApp.stop();
    Assert.assertTrue(allocator.isUnregistered());
  }
  
  // Step-1 : AM send allocate request for 2 ContainerRequests and 1
  // blackListeNode
  // Step-2 : 2 containers are allocated by RM.
  // Step-3 : AM Send 1 containerRequest(event3) and 1 releaseRequests to
  // RM
  // Step-4 : On RM restart, AM(does not know RM is restarted) sends
  // additional containerRequest(event4) and blacklisted nodes.
  // Intern RM send resync command
  // Step-5 : On Resync,AM sends all outstanding
  // asks,release,blacklistAaddition
  // and another containerRequest(event5)
  // Step-6 : RM allocates containers i.e event3,event4 and cRequest5
  @Test
  public void testRMContainerAllocatorResendsRequestsOnRMRestart()
      throws Exception {

    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.RECOVERY_ENABLED, "true");
    conf.set(YarnConfiguration.RM_STORE, MemoryRMStateStore.class.getName());
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
        YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
    conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, true);
    conf.setLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS, 0);
    conf.setBoolean(MRJobConfig.MR_AM_JOB_NODE_BLACKLISTING_ENABLE, true);
    conf.setInt(MRJobConfig.MAX_TASK_FAILURES_PER_TRACKER, 1);
    conf.setInt(
        MRJobConfig.MR_AM_IGNORE_BLACKLISTING_BLACKLISTED_NODE_PERECENT, -1);

    MemoryRMStateStore memStore = new MemoryRMStateStore();
    memStore.init(conf);

    MyResourceManager rm1 = new MyResourceManager(conf, memStore);
    rm1.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm1.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm1.submitApp(1024);
    dispatcher.await();

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm1.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator =
        new MyContainerAllocator(rm1, conf, appAttemptId, mockJob);

    // Step-1 : AM send allocate request for 2 ContainerRequests and 1
    // blackListeNode
    // create the container request
    // send MAP request
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 1024, new String[] { "h1" });
    allocator.sendRequest(event1);

    ContainerRequestEvent event2 =
        createReq(jobId, 2, 2048, new String[] { "h1", "h2" });
    allocator.sendRequest(event2);

    // Send events to blacklist h2
    ContainerFailedEvent f1 = createFailEvent(jobId, 1, "h2", false);
    allocator.sendFailure(f1);

    // send allocate request and 1 blacklisted nodes
    List<TaskAttemptContainerAssignedEvent> assignedContainers =
        allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 0", 0,
        assignedContainers.size());
    // Why ask is 3, not 4? --> ask from blacklisted node h2 is removed
    assertAsksAndReleases(3, 0, rm1);
    assertBlacklistAdditionsAndRemovals(1, 0, rm1);

    nm1.nodeHeartbeat(true); // Node heartbeat
    dispatcher.await();

    // Step-2 : 2 containers are allocated by RM.
    assignedContainers = allocator.schedule();
    dispatcher.await();
    Assert.assertEquals("No of assignments must be 2", 2,
        assignedContainers.size());
    assertAsksAndReleases(0, 0, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);

    assignedContainers = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0,
        assignedContainers.size());
    assertAsksAndReleases(3, 0, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);

    // Step-3 : AM Send 1 containerRequest(event3) and 1 releaseRequests to
    // RM
    // send container request
    ContainerRequestEvent event3 =
        createReq(jobId, 3, 1000, new String[] { "h1" });
    allocator.sendRequest(event3);

    // send deallocate request
    ContainerAllocatorEvent deallocate1 =
        createDeallocateEvent(jobId, 1, false);
    allocator.sendDeallocate(deallocate1);

    assignedContainers = allocator.schedule();
    Assert.assertEquals("No of assignments must be 0", 0,
        assignedContainers.size());
    assertAsksAndReleases(3, 1, rm1);
    assertBlacklistAdditionsAndRemovals(0, 0, rm1);

    // Phase-2 start 2nd RM is up
    MyResourceManager rm2 = new MyResourceManager(conf, memStore);
    rm2.start();
    nm1.setResourceTrackerService(rm2.getResourceTrackerService());
    allocator.updateSchedulerProxy(rm2);
    dispatcher = (DrainDispatcher) rm2.getRMContext().getDispatcher();

    // NM should be rebooted on heartbeat, even first heartbeat for nm2
    NodeHeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
    Assert.assertEquals(NodeAction.RESYNC, hbResponse.getNodeAction());

    // new NM to represent NM re-register
    nm1 = new MockNM("h1:1234", 10240, rm2.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true);
    dispatcher.await();

    // Step-4 : On RM restart, AM(does not know RM is restarted) sends
    // additional containerRequest(event4) and blacklisted nodes.
    // Intern RM send resync command

    // send deallocate request, release=1
    ContainerAllocatorEvent deallocate2 =
        createDeallocateEvent(jobId, 2, false);
    allocator.sendDeallocate(deallocate2);

    // Send events to blacklist nodes h3
    ContainerFailedEvent f2 = createFailEvent(jobId, 1, "h3", false);
    allocator.sendFailure(f2);

    ContainerRequestEvent event4 =
        createReq(jobId, 4, 2000, new String[] { "h1", "h2" });
    allocator.sendRequest(event4);

    // send allocate request to 2nd RM and get resync command
    allocator.schedule();
    dispatcher.await();

    // Step-5 : On Resync,AM sends all outstanding
    // asks,release,blacklistAaddition
    // and another containerRequest(event5)
    ContainerRequestEvent event5 =
        createReq(jobId, 5, 3000, new String[] { "h1", "h2", "h3" });
    allocator.sendRequest(event5);

    // send all outstanding request again.
    assignedContainers = allocator.schedule();
    dispatcher.await();
    assertAsksAndReleases(3, 2, rm2);
    assertBlacklistAdditionsAndRemovals(2, 0, rm2);

    nm1.nodeHeartbeat(true);
    dispatcher.await();

    // Step-6 : RM allocates containers i.e event3,event4 and cRequest5
    assignedContainers = allocator.schedule();
    dispatcher.await();

    Assert.assertEquals("Number of container should be 3", 3,
        assignedContainers.size());

    for (TaskAttemptContainerAssignedEvent assig : assignedContainers) {
      Assert.assertTrue("Assigned count not correct",
          "h1".equals(assig.getContainer().getNodeId().getHost()));
    }

    rm1.stop();
    rm2.stop();

  }

  @Test
  public void testUnsupportedMapContainerRequirement() throws Exception {
    final Resource maxContainerSupported = Resource.newInstance(1, 1);

    final ApplicationId appId = ApplicationId.newInstance(1, 1);
    final ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    final JobId jobId =
        MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);

    final MockScheduler mockScheduler = new MockScheduler(appAttemptId);
    final Configuration conf = new Configuration();

    final MyContainerAllocator allocator = new MyContainerAllocator(null,
        conf, appAttemptId, mock(Job.class), new SystemClock()) {
      @Override
      protected void register() {
      }
      @Override
      protected ApplicationMasterProtocol createSchedulerProxy() {
        return mockScheduler;
      }
      @Override
      protected Resource getMaxContainerCapability() {
        return maxContainerSupported;
      }
    };

    ContainerRequestEvent mapRequestEvt = createReq(jobId, 0,
        (int) (maxContainerSupported.getMemory() + 10),
        maxContainerSupported.getVirtualCores(),
        new String[0], false, false);
    allocator.sendRequests(Arrays.asList(mapRequestEvt));
    allocator.schedule();

    Assert.assertEquals(0, mockScheduler.lastAnyAskMap);
  }

  @Test
  public void testUnsupportedReduceContainerRequirement() throws Exception {
    final Resource maxContainerSupported = Resource.newInstance(1, 1);

    final ApplicationId appId = ApplicationId.newInstance(1, 1);
    final ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    final JobId jobId =
        MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);

    final MockScheduler mockScheduler = new MockScheduler(appAttemptId);
    final Configuration conf = new Configuration();

    final MyContainerAllocator allocator = new MyContainerAllocator(null,
        conf, appAttemptId, mock(Job.class), new SystemClock()) {
      @Override
      protected void register() {
      }
      @Override
      protected ApplicationMasterProtocol createSchedulerProxy() {
        return mockScheduler;
      }
      @Override
      protected Resource getMaxContainerCapability() {
        return maxContainerSupported;
      }
    };

    ContainerRequestEvent reduceRequestEvt = createReq(jobId, 0,
        (int) (maxContainerSupported.getMemory() + 10),
        maxContainerSupported.getVirtualCores(),
        new String[0], false, true);
    allocator.sendRequests(Arrays.asList(reduceRequestEvt));
    // Reducer container requests are added to the pending queue upon request,
    // schedule all reducers here so that we can observe if reducer requests
    // are accepted by RMContainerAllocator on RM side.
    allocator.scheduleAllReduces();
    allocator.schedule();

    Assert.assertEquals(0, mockScheduler.lastAnyAskReduce);
  }

  @Test
  public void testRMUnavailable()
      throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(
      MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS, 0);
    MyResourceManager rm1 = new MyResourceManager(conf);
    rm1.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm1.getRMContext().getDispatcher();
    RMApp app = rm1.submitApp(1024);
    dispatcher.await();

    MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
    nm1.registerNode();
    nm1.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId =
        app.getCurrentAppAttempt().getAppAttemptId();
    rm1.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
        0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator2 allocator =
        new MyContainerAllocator2(rm1, conf, appAttemptId, mockJob);
    allocator.jobEvents.clear();
    try {
      allocator.schedule();
      Assert.fail("Should Have Exception");
    } catch (RMContainerAllocationException e) {
      Assert.assertTrue(e.getMessage().contains("Could not contact RM after"));
    }
    dispatcher.await();
    Assert.assertEquals("Should Have 1 Job Event", 1,
        allocator.jobEvents.size());
    JobEvent event = allocator.jobEvents.get(0); 
    Assert.assertTrue("Should Reboot", event.getType().equals(JobEventType.JOB_AM_REBOOT));
  }

  @Test(timeout=60000)
  public void testAMRMTokenUpdate() throws Exception {
    LOG.info("Running testAMRMTokenUpdate");

    final String rmAddr = "somermaddress:1234";
    final Configuration conf = new YarnConfiguration();
    conf.setLong(
      YarnConfiguration.RM_AMRM_TOKEN_MASTER_KEY_ROLLING_INTERVAL_SECS, 8);
    conf.setLong(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 2000);
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddr);

    final MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    AMRMTokenSecretManager secretMgr =
        rm.getRMContext().getAMRMTokenSecretManager();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    final ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    final ApplicationId appId = app.getApplicationId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    final Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));

    final Token<AMRMTokenIdentifier> oldToken = rm.getRMContext().getRMApps()
        .get(appId).getRMAppAttempt(appAttemptId).getAMRMToken();
    Assert.assertNotNull("app should have a token", oldToken);
    UserGroupInformation testUgi = UserGroupInformation.createUserForTesting(
        "someuser", new String[0]);
    Token<AMRMTokenIdentifier> newToken = testUgi.doAs(
        new PrivilegedExceptionAction<Token<AMRMTokenIdentifier>>() {
          @Override
          public Token<AMRMTokenIdentifier> run() throws Exception {
            MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
                appAttemptId, mockJob);

            // Keep heartbeating until RM thinks the token has been updated
            Token<AMRMTokenIdentifier> currentToken = oldToken;
            long startTime = Time.monotonicNow();
            while (currentToken == oldToken) {
              if (Time.monotonicNow() - startTime > 20000) {
                Assert.fail("Took to long to see AMRM token change");
              }
              Thread.sleep(100);
              allocator.schedule();
              currentToken = rm.getRMContext().getRMApps().get(appId)
                  .getRMAppAttempt(appAttemptId).getAMRMToken();
            }

            return currentToken;
          }
        });

    // verify there is only one AMRM token in the UGI and it matches the
    // updated token from the RM
    int tokenCount = 0;
    Token<? extends TokenIdentifier> ugiToken = null;
    for (Token<? extends TokenIdentifier> token : testUgi.getTokens()) {
      if (AMRMTokenIdentifier.KIND_NAME.equals(token.getKind())) {
        ugiToken = token;
        ++tokenCount;
      }
    }

    Assert.assertEquals("too many AMRM tokens", 1, tokenCount);
    Assert.assertArrayEquals("token identifier not updated",
        newToken.getIdentifier(), ugiToken.getIdentifier());
    Assert.assertArrayEquals("token password not updated",
        newToken.getPassword(), ugiToken.getPassword());
    Assert.assertEquals("AMRM token service not updated",
        new Text(rmAddr), ugiToken.getService());
  }

  @Test(expected = RMContainerAllocationException.class)
  public void testAttemptNotFoundCausesRMCommunicatorException()
      throws Exception {

    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher = (DrainDispatcher) rm.getRMContext()
        .getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 2048);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    // Now kill the application
    rm.killApp(app.getApplicationId());
    rm.waitForState(app.getApplicationId(), RMAppState.KILLED);
    allocator.schedule();
  }

  @Test
  public void testConcurrentTaskLimits() throws Exception {
    final int MAP_LIMIT = 3;
    final int REDUCE_LIMIT = 1;
    LOG.info("Running testConcurrentTaskLimits");
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT, MAP_LIMIT);
    conf.setInt(MRJobConfig.JOB_RUNNING_REDUCE_LIMIT, REDUCE_LIMIT);
    conf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 1.0f);
    ApplicationId appId = ApplicationId.newInstance(1, 1);
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(
        appId, 1);
    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    final MockScheduler mockScheduler = new MockScheduler(appAttemptId);
    MyContainerAllocator allocator = new MyContainerAllocator(null, conf,
        appAttemptId, mockJob) {
      @Override
      protected void register() {
      }

      @Override
      protected ApplicationMasterProtocol createSchedulerProxy() {
        return mockScheduler;
      }
    };

    // create some map requests
    ContainerRequestEvent[] reqMapEvents = new ContainerRequestEvent[5];
    for (int i = 0; i < reqMapEvents.length; ++i) {
      reqMapEvents[i] = createReq(jobId, i, 1024, new String[]{"h" + i});
    }
    allocator.sendRequests(Arrays.asList(reqMapEvents));

    // create some reduce requests
    ContainerRequestEvent[] reqReduceEvents = new ContainerRequestEvent[2];
    for (int i = 0; i < reqReduceEvents.length; ++i) {
      reqReduceEvents[i] = createReq(jobId, i, 1024, new String[]{},
          false, true);
    }
    allocator.sendRequests(Arrays.asList(reqReduceEvents));
    allocator.schedule();

    // verify all of the host-specific asks were sent plus one for the
    // default rack and one for the ANY request
    Assert.assertEquals(reqMapEvents.length + 2, mockScheduler.lastAsk.size());

    // verify AM is only asking for the map limit overall
    Assert.assertEquals(MAP_LIMIT, mockScheduler.lastAnyAskMap);

    // assign a map task and verify we do not ask for any more maps
    ContainerId cid0 = mockScheduler.assignContainer("h0", false);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(2, mockScheduler.lastAnyAskMap);

    // complete the map task and verify that we ask for one more
    mockScheduler.completeContainer(cid0);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(3, mockScheduler.lastAnyAskMap);

    // assign three more maps and verify we ask for no more maps
    ContainerId cid1 = mockScheduler.assignContainer("h1", false);
    ContainerId cid2 = mockScheduler.assignContainer("h2", false);
    ContainerId cid3 = mockScheduler.assignContainer("h3", false);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskMap);

    // complete two containers and verify we only asked for one more
    // since at that point all maps should be scheduled/completed
    mockScheduler.completeContainer(cid2);
    mockScheduler.completeContainer(cid3);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(1, mockScheduler.lastAnyAskMap);

    // allocate the last container and complete the first one
    // and verify there are no more map asks.
    mockScheduler.completeContainer(cid1);
    ContainerId cid4 = mockScheduler.assignContainer("h4", false);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskMap);

    // complete the last map
    mockScheduler.completeContainer(cid4);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskMap);

    // verify only reduce limit being requested
    Assert.assertEquals(REDUCE_LIMIT, mockScheduler.lastAnyAskReduce);

    // assign a reducer and verify ask goes to zero
    cid0 = mockScheduler.assignContainer("h0", true);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskReduce);

    // complete the reducer and verify we ask for another
    mockScheduler.completeContainer(cid0);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(1, mockScheduler.lastAnyAskReduce);

    // assign a reducer and verify ask goes to zero
    cid0 = mockScheduler.assignContainer("h0", true);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskReduce);

    // complete the reducer and verify no more reducers
    mockScheduler.completeContainer(cid0);
    allocator.schedule();
    allocator.schedule();
    Assert.assertEquals(0, mockScheduler.lastAnyAskReduce);
    allocator.close();
  }

  @Test
  public void testUpdateAskOnRampDownAllReduces() throws Exception {
    LOG.info("Running testUpdateAskOnRampDownAllReduces");
    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 1260);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);
    // Use a controlled clock to advance time for test.
    ControlledClock clock = (ControlledClock)allocator.getContext().getClock();
    clock.setTime(System.currentTimeMillis());

    // Register nodes to RM.
    MockNM nodeManager = rm.registerNode("h1:1234", 1024);
    dispatcher.await();

    // Request 2 maps and 1 reducer(sone on nodes which are not registered).
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 1024, new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 =
        createReq(jobId, 2, 1024, new String[] { "h2" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 =
        createReq(jobId, 3, 1024, new String[] { "h2" }, false, true);
    allocator.sendRequest(event3);

    // This will tell the scheduler about the requests but there will be no
    // allocations as nodes are not added.
    allocator.schedule();
    dispatcher.await();

    // Advance clock so that maps can be considered as hanging.
    clock.setTime(System.currentTimeMillis() + 500000L);

    // Request for another reducer on h3 which has not registered.
    ContainerRequestEvent event4 =
        createReq(jobId, 4, 1024, new String[] { "h3" }, false, true);
    allocator.sendRequest(event4);

    allocator.schedule();
    dispatcher.await();

    // Update resources in scheduler through node heartbeat from h1.
    nodeManager.nodeHeartbeat(true);
    dispatcher.await();

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(1024, 1));
    allocator.schedule();
    dispatcher.await();

    // One map is assigned.
    Assert.assertEquals(1, allocator.getAssignedRequests().maps.size());
    // Send deallocate request for map so that no maps are assigned after this.
    ContainerAllocatorEvent deallocate = createDeallocateEvent(jobId, 1, false);
    allocator.sendDeallocate(deallocate);
    // Now one reducer should be scheduled and one should be pending.
    Assert.assertEquals(1, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(1, allocator.getNumOfPendingReduces());
    // No map should be assigned and one should be scheduled.
    Assert.assertEquals(1, allocator.getScheduledRequests().maps.size());
    Assert.assertEquals(0, allocator.getAssignedRequests().maps.size());

    Assert.assertEquals(6, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      boolean isReduce =
          req.getPriority().equals(RMContainerAllocator.PRIORITY_REDUCE);
      if (isReduce) {
        // 1 reducer each asked on h2, * and default-rack
        Assert.assertTrue((req.getResourceName().equals("*") ||
            req.getResourceName().equals("/default-rack") ||
            req.getResourceName().equals("h2")) && req.getNumContainers() == 1);
      } else { //map
        // 0 mappers asked on h1 and 1 each on * and default-rack
        Assert.assertTrue(((req.getResourceName().equals("*") ||
            req.getResourceName().equals("/default-rack")) &&
            req.getNumContainers() == 1) || (req.getResourceName().equals("h1")
            && req.getNumContainers() == 0));
      }
    }
    // On next allocate request to scheduler, headroom reported will be 0.
    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(0, 0));
    allocator.schedule();
    dispatcher.await();
    // After allocate response from scheduler, all scheduled reduces are ramped
    // down and move to pending. 3 asks are also updated with 0 containers to
    // indicate ramping down of reduces to scheduler.
    Assert.assertEquals(0, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(2, allocator.getNumOfPendingReduces());
    Assert.assertEquals(3, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      Assert.assertEquals(
          RMContainerAllocator.PRIORITY_REDUCE, req.getPriority());
      Assert.assertTrue(req.getResourceName().equals("*") ||
          req.getResourceName().equals("/default-rack") ||
          req.getResourceName().equals("h2"));
      Assert.assertEquals(Resource.newInstance(1024, 1), req.getCapability());
      Assert.assertEquals(0, req.getNumContainers());
    }
  }

  /**
   * MAPREDUCE-6771. Test if RMContainerAllocator generates the events in the
   * right order while processing finished containers.
   */
  @Test
  public void testHandlingFinishedContainers() {
    EventHandler eventHandler = mock(EventHandler.class);

    AppContext context = mock(MRAppMaster.RunningAppContext.class);
    when(context.getClock()).thenReturn(new ControlledClock());
    when(context.getClusterInfo()).thenReturn(
        new ClusterInfo(Resource.newInstance(10240, 1)));
    when(context.getEventHandler()).thenReturn(eventHandler);
    RMContainerAllocator containerAllocator =
        new RMContainerAllocatorForFinishedContainer(null, context);

    ContainerStatus finishedContainer = ContainerStatus.newInstance(
        mock(ContainerId.class), ContainerState.COMPLETE, "", 0);
    containerAllocator.processFinishedContainer(finishedContainer);

    InOrder inOrder = inOrder(eventHandler);
    inOrder.verify(eventHandler).handle(
        isA(TaskAttemptDiagnosticsUpdateEvent.class));
    inOrder.verify(eventHandler).handle(isA(TaskAttemptEvent.class));
    inOrder.verifyNoMoreInteractions();
  }

  private static class RMContainerAllocatorForFinishedContainer
      extends RMContainerAllocator {
    public RMContainerAllocatorForFinishedContainer(ClientService clientService,
        AppContext context) {
      super(clientService, context);
    }
    @Override
    protected AssignedRequests createAssignedRequests() {
      AssignedRequests assignedReqs = mock(AssignedRequests.class);
      TaskAttemptId taskAttempt = mock(TaskAttemptId.class);
      when(assignedReqs.get(any(ContainerId.class))).thenReturn(taskAttempt);
      return assignedReqs;
    }
  }

  @Test
  public void testAvoidAskMoreReducersWhenReducerPreemptionIsRequired()
      throws Exception {
    LOG.info("Running testAvoidAskMoreReducersWhenReducerPreemptionIsRequired");
    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 1260);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);
    // Use a controlled clock to advance time for test.
    ControlledClock clock = (ControlledClock)allocator.getContext().getClock();
    clock.setTime(System.currentTimeMillis());

    // Register nodes to RM.
    MockNM nodeManager = rm.registerNode("h1:1234", 1024);
    dispatcher.await();

    // Request 2 maps and 1 reducer(sone on nodes which are not registered).
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 1024, new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 =
        createReq(jobId, 2, 1024, new String[] { "h2" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 =
        createReq(jobId, 3, 1024, new String[] { "h2" }, false, true);
    allocator.sendRequest(event3);

    // This will tell the scheduler about the requests but there will be no
    // allocations as nodes are not added.
    allocator.schedule();
    dispatcher.await();

    // Advance clock so that maps can be considered as hanging.
    clock.setTime(System.currentTimeMillis() + 500000L);

    // Request for another reducer on h3 which has not registered.
    ContainerRequestEvent event4 =
        createReq(jobId, 4, 1024, new String[] { "h3" }, false, true);
    allocator.sendRequest(event4);

    allocator.schedule();
    dispatcher.await();

    // Update resources in scheduler through node heartbeat from h1.
    nodeManager.nodeHeartbeat(true);
    dispatcher.await();

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(1024, 1));
    allocator.schedule();
    dispatcher.await();

    // One map is assigned.
    Assert.assertEquals(1, allocator.getAssignedRequests().maps.size());
    // Send deallocate request for map so that no maps are assigned after this.
    ContainerAllocatorEvent deallocate = createDeallocateEvent(jobId, 1, false);
    allocator.sendDeallocate(deallocate);
    // Now one reducer should be scheduled and one should be pending.
    Assert.assertEquals(1, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(1, allocator.getNumOfPendingReduces());
    // No map should be assigned and one should be scheduled.
    Assert.assertEquals(1, allocator.getScheduledRequests().maps.size());
    Assert.assertEquals(0, allocator.getAssignedRequests().maps.size());

    Assert.assertEquals(6, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      boolean isReduce =
          req.getPriority().equals(RMContainerAllocator.PRIORITY_REDUCE);
      if (isReduce) {
        // 1 reducer each asked on h2, * and default-rack
        Assert.assertTrue((req.getResourceName().equals("*") ||
            req.getResourceName().equals("/default-rack") ||
            req.getResourceName().equals("h2")) && req.getNumContainers() == 1);
      } else { //map
        // 0 mappers asked on h1 and 1 each on * and default-rack
        Assert.assertTrue(((req.getResourceName().equals("*") ||
            req.getResourceName().equals("/default-rack")) &&
            req.getNumContainers() == 1) || (req.getResourceName().equals("h1")
            && req.getNumContainers() == 0));
      }
    }

    clock.setTime(System.currentTimeMillis() + 500000L + 10 * 60 * 1000);

    // On next allocate request to scheduler, headroom reported will be 2048.
    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(2048, 0));
    allocator.schedule();
    dispatcher.await();
    // After allocate response from scheduler, all scheduled reduces are ramped
    // down and move to pending. 3 asks are also updated with 0 containers to
    // indicate ramping down of reduces to scheduler.
    Assert.assertEquals(0, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(2, allocator.getNumOfPendingReduces());
    Assert.assertEquals(3, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      Assert.assertEquals(
          RMContainerAllocator.PRIORITY_REDUCE, req.getPriority());
      Assert.assertTrue(req.getResourceName().equals("*") ||
          req.getResourceName().equals("/default-rack") ||
          req.getResourceName().equals("h2"));
      Assert.assertEquals(Resource.newInstance(1024, 1), req.getCapability());
      Assert.assertEquals(0, req.getNumContainers());
    }
  }

  /**
   * Tests whether scheduled reducers are excluded from headroom while
   * calculating headroom.
   */
  @Test
  public void testExcludeSchedReducesFromHeadroom() throws Exception {
    LOG.info("Running testExcludeSchedReducesFromHeadroom");
    Configuration conf = new Configuration();
    conf.setInt(MRJobConfig.MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC, -1);
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
        (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 1260);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
        .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
        MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
            0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    Task mockTask = mock(Task.class);
    TaskAttempt mockTaskAttempt = mock(TaskAttempt.class);
    when(mockJob.getTask((TaskId)any())).thenReturn(mockTask);
    when(mockTask.getAttempt((TaskAttemptId)any())).thenReturn(mockTaskAttempt);
    when(mockTaskAttempt.getProgress()).thenReturn(0.01f);
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
        appAttemptId, mockJob);

    MockNM nodeManager = rm.registerNode("h1:1234", 4096);
    dispatcher.await();
    // Register nodes to RM.
    MockNM nodeManager2 = rm.registerNode("h2:1234", 1024);
    dispatcher.await();

    // Request 2 maps and 1 reducer(sone on nodes which are not registered).
    ContainerRequestEvent event1 =
        createReq(jobId, 1, 1024, new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 =
        createReq(jobId, 2, 1024, new String[] { "h2" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 =
         createReq(jobId, 3, 1024, new String[] { "h1" }, false, true);
    allocator.sendRequest(event3);

    // This will tell the scheduler about the requests but there will be no
    // allocations as nodes are not added.
    allocator.schedule();
    dispatcher.await();

    // Request for another reducer on h3 which has not registered.
    ContainerRequestEvent event4 =
        createReq(jobId, 4, 1024, new String[] { "h3" }, false, true);
    allocator.sendRequest(event4);

    allocator.schedule();
    dispatcher.await();

    // Update resources in scheduler through node heartbeat from h1.
    nodeManager.nodeHeartbeat(true);
    dispatcher.await();

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(3072, 3));
    allocator.schedule();
    dispatcher.await();

    // Two maps are assigned.
    Assert.assertEquals(2, allocator.getAssignedRequests().maps.size());
    // Send deallocate request for map so that no maps are assigned after this.
    ContainerAllocatorEvent deallocate1 = createDeallocateEvent(jobId, 1, false);
    allocator.sendDeallocate(deallocate1);
    ContainerAllocatorEvent deallocate2 = createDeallocateEvent(jobId, 2, false);
    allocator.sendDeallocate(deallocate2);
    // No map should be assigned.
    Assert.assertEquals(0, allocator.getAssignedRequests().maps.size());

    nodeManager.nodeHeartbeat(true);
    dispatcher.await();

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(1024, 1));
    allocator.schedule();
    dispatcher.await();

    // h2 heartbeats.
    nodeManager2.nodeHeartbeat(true);
    dispatcher.await();

    // Send request for one more mapper.
    ContainerRequestEvent event5 =
        createReq(jobId, 5, 1024, new String[] { "h1" });
    allocator.sendRequest(event5);

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(2048, 2));
    allocator.schedule();
    dispatcher.await();
    // One reducer is assigned and one map is scheduled
    Assert.assertEquals(1, allocator.getScheduledRequests().maps.size());
    Assert.assertEquals(1, allocator.getAssignedRequests().reduces.size());
    // Headroom enough to run a mapper if headroom is taken as it is but wont be
    // enough if scheduled reducers resources are deducted.
    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(1260, 2));
    allocator.schedule();
    dispatcher.await();
    // After allocate response, the one assigned reducer is preempted and killed
    Assert.assertEquals(1, MyContainerAllocator.getTaskAttemptKillEvents().size());
    Assert.assertEquals(RMContainerAllocator.RAMPDOWN_DIAGNOSTIC,
        MyContainerAllocator.getTaskAttemptKillEvents().get(0).getMessage());
    Assert.assertEquals(1, allocator.getNumOfPendingReduces());
  }

  private static class MockScheduler implements ApplicationMasterProtocol {
    ApplicationAttemptId attemptId;
    long nextContainerId = 10;
    List<ResourceRequest> lastAsk = null;
    int lastAnyAskMap = 0;
    int lastAnyAskReduce = 0;
    List<ContainerStatus> containersToComplete =
        new ArrayList<ContainerStatus>();
    List<Container> containersToAllocate = new ArrayList<Container>();

    public MockScheduler(ApplicationAttemptId attemptId) {
      this.attemptId = attemptId;
    }

    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster(
        RegisterApplicationMasterRequest request) throws YarnException,
        IOException {
      return RegisterApplicationMasterResponse.newInstance(
          Resource.newInstance(512, 1),
          Resource.newInstance(512000, 1024),
          Collections.<ApplicationAccessType,String>emptyMap(),
          ByteBuffer.wrap("fake_key".getBytes()),
          Collections.<Container>emptyList(),
          "default",
          Collections.<NMToken>emptyList());
    }

    @Override
    public FinishApplicationMasterResponse finishApplicationMaster(
        FinishApplicationMasterRequest request) throws YarnException,
        IOException {
      return FinishApplicationMasterResponse.newInstance(false);
    }

    @Override
    public AllocateResponse allocate(AllocateRequest request)
        throws YarnException, IOException {
      lastAsk = request.getAskList();
      for (ResourceRequest req : lastAsk) {
        if (ResourceRequest.ANY.equals(req.getResourceName())) {
          Priority priority = req.getPriority();
          if (priority.equals(RMContainerAllocator.PRIORITY_MAP)) {
            lastAnyAskMap = req.getNumContainers();
          } else if (priority.equals(RMContainerAllocator.PRIORITY_REDUCE)){
            lastAnyAskReduce = req.getNumContainers();
          }
        }
      }
      AllocateResponse response =  AllocateResponse.newInstance(
          request.getResponseId(),
          containersToComplete, containersToAllocate,
          Collections.<NodeReport>emptyList(),
          Resource.newInstance(512000, 1024), null, 10, null,
          Collections.<NMToken>emptyList());
      containersToComplete.clear();
      containersToAllocate.clear();
      return response;
    }

    public ContainerId assignContainer(String nodeName, boolean isReduce) {
      ContainerId containerId =
          ContainerId.newContainerId(attemptId, nextContainerId++);
      Priority priority = isReduce ? RMContainerAllocator.PRIORITY_REDUCE
          : RMContainerAllocator.PRIORITY_MAP;
      Container container = Container.newInstance(containerId,
          NodeId.newInstance(nodeName, 1234), nodeName + ":5678",
        Resource.newInstance(1024, 1), priority, null);
      containersToAllocate.add(container);
      return containerId;
    }

    public void completeContainer(ContainerId containerId) {
      containersToComplete.add(ContainerStatus.newInstance(containerId,
          ContainerState.COMPLETE, "", 0));
    }
  }

  @Test
  public void testUpdateAskOnRampDownAllReduces() throws Exception {
    LOG.info("Running testUpdateAskOnRampDownAllReduces");
    Configuration conf = new Configuration();
    MyResourceManager rm = new MyResourceManager(conf);
    rm.start();
    DrainDispatcher dispatcher =
            (DrainDispatcher) rm.getRMContext().getDispatcher();

    // Submit the application
    RMApp app = rm.submitApp(1024);
    dispatcher.await();

    MockNM amNodeManager = rm.registerNode("amNM:1234", 1260);
    amNodeManager.nodeHeartbeat(true);
    dispatcher.await();

    ApplicationAttemptId appAttemptId = app.getCurrentAppAttempt()
            .getAppAttemptId();
    rm.sendAMLaunched(appAttemptId);
    dispatcher.await();

    JobId jobId = MRBuilderUtils.newJobId(appAttemptId.getApplicationId(), 0);
    Job mockJob = mock(Job.class);
    when(mockJob.getReport()).thenReturn(
            MRBuilderUtils.newJobReport(jobId, "job", "user", JobState.RUNNING, 0,
                    0, 0, 0, 0, 0, 0, "jobfile", null, false, ""));
    MyContainerAllocator allocator = new MyContainerAllocator(rm, conf,
            appAttemptId, mockJob);
    // Use a controlled clock to advance time for test.
    ControlledClock clock = (ControlledClock)allocator.getContext().getClock();
    clock.setTime(System.currentTimeMillis());

    // Register nodes to RM.
    MockNM nodeManager = rm.registerNode("h1:1234", 1024);
    dispatcher.await();

    // Request 2 maps and 1 reducer(sone on nodes which are not registered).
    ContainerRequestEvent event1 =
            createReq(jobId, 1, 1024, new String[] { "h1" });
    allocator.sendRequest(event1);
    ContainerRequestEvent event2 =
            createReq(jobId, 2, 1024, new String[] { "h2" });
    allocator.sendRequest(event2);
    ContainerRequestEvent event3 =
            createReq(jobId, 3, 1024, new String[] { "h2" }, false, true);
    allocator.sendRequest(event3);

    // This will tell the scheduler about the requests but there will be no
    // allocations as nodes are not added.
    allocator.schedule();
    dispatcher.await();

    // Advance clock so that maps can be considered as hanging.
    clock.setTime(System.currentTimeMillis() + 500000L);

    // Request for another reducer on h3 which has not registered.
    ContainerRequestEvent event4 =
            createReq(jobId, 4, 1024, new String[] { "h3" }, false, true);
    allocator.sendRequest(event4);

    allocator.schedule();
    dispatcher.await();

    // Update resources in scheduler through node heartbeat from h1.
    nodeManager.nodeHeartbeat(true);
    dispatcher.await();

    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(1024, 1));
    allocator.schedule();
    dispatcher.await();

    // One map is assigned.
    Assert.assertEquals(1, allocator.getAssignedRequests().maps.size());
    // Send deallocate request for map so that no maps are assigned after this.
    ContainerAllocatorEvent deallocate = createDeallocateEvent(jobId, 1, false);
    allocator.sendDeallocate(deallocate);
    // Now one reducer should be scheduled and one should be pending.
    Assert.assertEquals(1, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(1, allocator.getNumOfPendingReduces());
    // No map should be assigned and one should be scheduled.
    Assert.assertEquals(1, allocator.getScheduledRequests().maps.size());
    Assert.assertEquals(0, allocator.getAssignedRequests().maps.size());

    Assert.assertEquals(6, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      boolean isReduce =
              req.getPriority().equals(RMContainerAllocator.PRIORITY_REDUCE);
      if (isReduce) {
        // 1 reducer each asked on h2, * and default-rack
        Assert.assertTrue((req.getResourceName().equals("*") ||
                req.getResourceName().equals("/default-rack") ||
                req.getResourceName().equals("h2")) && req.getNumContainers() == 1);
      } else { //map
        // 0 mappers asked on h1 and 1 each on * and default-rack
        Assert.assertTrue(((req.getResourceName().equals("*") ||
                req.getResourceName().equals("/default-rack")) &&
                req.getNumContainers() == 1) || (req.getResourceName().equals("h1")
                && req.getNumContainers() == 0));
      }
    }
    // On next allocate request to scheduler, headroom reported will be 0.
    rm.getMyFifoScheduler().forceResourceLimit(Resource.newInstance(0, 0));
    allocator.schedule();
    dispatcher.await();
    // After allocate response from scheduler, all scheduled reduces are ramped
    // down and move to pending. 3 asks are also updated with 0 containers to
    // indicate ramping down of reduces to scheduler.
    Assert.assertEquals(0, allocator.getScheduledRequests().reduces.size());
    Assert.assertEquals(2, allocator.getNumOfPendingReduces());
    Assert.assertEquals(3, allocator.getAsk().size());
    for (ResourceRequest req : allocator.getAsk()) {
      Assert.assertEquals(
              RMContainerAllocator.PRIORITY_REDUCE, req.getPriority());
      Assert.assertTrue(req.getResourceName().equals("*") ||
              req.getResourceName().equals("/default-rack") ||
              req.getResourceName().equals("h2"));
      Assert.assertEquals(Resource.newInstance(1024, 1), req.getCapability());
      Assert.assertEquals(0, req.getNumContainers());
    }
  }

  public static void main(String[] args) throws Exception {
    TestRMContainerAllocator t = new TestRMContainerAllocator();
    t.testSimple();
    t.testResource();
    t.testMapReduceScheduling();
    t.testReportedAppProgress();
    t.testReportedAppProgressWithOnlyMaps();
    t.testBlackListedNodes();
    t.testCompletedTasksRecalculateSchedule();
    t.testAMRMTokenUpdate();
  }

}
