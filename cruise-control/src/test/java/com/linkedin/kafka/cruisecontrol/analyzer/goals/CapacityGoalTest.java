/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.common.Resource;
import com.linkedin.kafka.cruisecontrol.config.BrokerCapacityInfo;
import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.monitor.ModelGeneration;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUnitTestUtils.getAggregatedMetricValues;
import static org.junit.Assert.assertEquals;

public class CapacityGoalTest {

    private static final TopicPartition TP0 = new TopicPartition("foobar1", 0);
    private static final TopicPartition TP1 = new TopicPartition("foobar2", 0);
    private static final BrokerCapacityInfo BROKER_CAPACITIES = new BrokerCapacityInfo(Map.of(
            Resource.CPU, (double) 2,
            Resource.NW_IN, (double) 100,
            Resource.NW_OUT, (double) 100,
            Resource.DISK, (double) 600
    ));
    private static final List<Long> WINDOWS = Collections.singletonList(1L);

    @Test
    public void testIsSwapAcceptableForCapacityChecksIntermediateState() {
        ClusterModel cm = new ClusterModel(new ModelGeneration(0, 0), 1.0);

        cm.createRack("r0");
        cm.createBroker("r0", "h0", 0, BROKER_CAPACITIES, false);
        cm.createBroker("r0", "h1", 1, BROKER_CAPACITIES, false);

        // Replica-set for foobar1-0: [0,1]
        cm.createReplica("r0", 0, TP0, 0, true);
        cm.setReplicaLoad("r0", 0, TP0, getAggregatedMetricValues(10.0, 10.0, 10.0, 235.0), WINDOWS);
        cm.createReplica("r0", 1, TP0, 1, false);
        cm.setReplicaLoad("r0", 1, TP0, getAggregatedMetricValues(10.0, 10.0, 10.0, 235.0), WINDOWS);

        // Replica-set for foobar2-0: [1,0]
        cm.createReplica("r0", 1, TP1, 0, true);
        cm.setReplicaLoad("r0", 1, TP1, getAggregatedMetricValues(10.0, 10.0, 10.0, 235.0), WINDOWS);
        cm.createReplica("r0", 0, TP1, 1, false);
        cm.setReplicaLoad("r0", 0, TP1, getAggregatedMetricValues(10.0, 10.0, 10.0, 235.0), WINDOWS);

        BalancingAction action = new BalancingAction(TP0, 0, 1, ActionType.INTER_BROKER_REPLICA_SWAP, TP1);

        Properties props = KafkaCruiseControlUnitTestUtils.getKafkaCruiseControlProperties();
        BalancingConstraint balancingConstraint = new BalancingConstraint(new KafkaCruiseControlConfig(props));

        DiskCapacityGoal goal = new DiskCapacityGoal(balancingConstraint);
        assertEquals(ActionAcceptance.REPLICA_REJECT, goal.actionAcceptance(action, cm));
    }
}
