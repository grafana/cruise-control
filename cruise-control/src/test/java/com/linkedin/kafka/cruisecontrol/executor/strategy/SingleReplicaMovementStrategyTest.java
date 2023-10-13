/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class SingleReplicaMovementStrategyTest {
    @Test
    public void testExpandExecutionTasks() {
        TopicPartition tp0 = new TopicPartition("foobar", 0);
        TopicPartition tp1 = new TopicPartition("foobar", 1);
        TopicPartition tp2 = new TopicPartition("foobar", 2);
        TopicPartition tp3 = new TopicPartition("foobar", 3);
        TopicPartition tp4 = new TopicPartition("foobar", 4);
        TopicPartition tp5 = new TopicPartition("foobar", 5);
        TopicPartition tp6 = new TopicPartition("foobar", 6);
        TopicPartition tp7 = new TopicPartition("foobar", 7);

        // AbstractReplicaMovementStrategy.expandExecutionTasks takes an unordered set, but we give it an
        // ordered set here to make it easier to test.
        SortedSet<ExecutionTask> tasks = new TreeSet<>();
        // Completely replace the replica set (3 replica movements).
        tasks.add(new ExecutionTask(
                0,
                new ExecutionProposal(
                        tp0,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(4, 5, 6)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing a follower (1 replica movement).
        tasks.add(new ExecutionTask(
                1,
                new ExecutionProposal(
                        tp1,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 2, 4)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing 2 followers (2 replica movements).
        tasks.add(new ExecutionTask(
                2,
                new ExecutionProposal(
                        tp2,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 4, 5)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing the leader (1 replica movement).
        tasks.add(new ExecutionTask(
                3,
                new ExecutionProposal(
                        tp3,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(4, 2, 3)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=3 to RF=2 (1 replica movement).
        tasks.add(new ExecutionTask(
                4,
                new ExecutionProposal(
                        tp4,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=4 to RF=2 (still 1 replica movement).
        tasks.add(new ExecutionTask(
                5,
                new ExecutionProposal(
                        tp5,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3, 4),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=1 to RF=2 (1 replica movement).
        tasks.add(new ExecutionTask(
                6,
                new ExecutionProposal(
                        tp6,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=1 to RF=3 (2 replica movements).
        tasks.add(new ExecutionTask(
                7,
                new ExecutionProposal(
                        tp7,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1),
                        replicaSet(1, 2, 3)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));

        List<ExecutionTask> actual = new ArrayList<>(SingleReplicaMovementStrategy.expandExecutionTasks(tasks));

        List<ExecutionTask> expected = new ArrayList<>();
        // Completely replace the replica set (3 replica movements).
        expected.add(new ExecutionTask(
                0,
                new ExecutionProposal(
                        tp0,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(4, 3, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        expected.add(new ExecutionTask(
                1,
                new ExecutionProposal(
                        tp0,
                        20,
                        new ReplicaPlacementInfo(4),
                        replicaSet(4, 3, 2),
                        replicaSet(4, 2, 6)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        expected.add(new ExecutionTask(
                2,
                new ExecutionProposal(
                        tp0,
                        20,
                        new ReplicaPlacementInfo(4),
                        replicaSet(4, 2, 6),
                        replicaSet(4, 6, 5)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing a follower (1 replica movement).
        expected.add(new ExecutionTask(
                3,
                new ExecutionProposal(
                        tp1,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 2, 4)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing 2 followers (2 replica movements).
        expected.add(new ExecutionTask(
                4,
                new ExecutionProposal(
                        tp2,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 2, 5)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        expected.add(new ExecutionTask(
                5,
                new ExecutionProposal(
                        tp2,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 5),
                        replicaSet(1, 5, 4)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Replacing the leader (1 replica movement).
        expected.add(new ExecutionTask(
                6,
                new ExecutionProposal(
                        tp3,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(4, 2, 3)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=3 to RF=2 (1 replica movement).
        expected.add(new ExecutionTask(
                7,
                new ExecutionProposal(
                        tp4,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=4 to RF=2 (still 1 replica movement).
        expected.add(new ExecutionTask(
                8,
                new ExecutionProposal(
                        tp5,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 2, 3, 4),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=1 to RF=2 (1 replica movement).
        expected.add(new ExecutionTask(
                9,
                new ExecutionProposal(
                        tp6,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1),
                        replicaSet(1, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        // Changing from RF=1 to RF=3 (2 replica movements).
        expected.add(new ExecutionTask(
                10,
                new ExecutionProposal(
                        tp7,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1),
                        replicaSet(1, 3)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));
        expected.add(new ExecutionTask(
                11,
                new ExecutionProposal(
                        tp7,
                        20,
                        new ReplicaPlacementInfo(1),
                        replicaSet(1, 3),
                        replicaSet(1, 3, 2)),
                ExecutionTask.TaskType.INTER_BROKER_REPLICA_ACTION,
                1000));

        Assert.assertEquals(expected.size(), actual.size());

        for (int i = 0; i < expected.size(); i++) {
            Assert.assertEquals(expected.get(i).executionId(), actual.get(i).executionId());
            Assert.assertEquals(expected.get(i).type(), actual.get(i).type());
            Assert.assertEquals(expected.get(i).alertTimeMs(), actual.get(i).alertTimeMs());

            Assert.assertEquals(expected.get(i).proposal(), actual.get(i).proposal());
        }
    }

    @Test
    public void testThrowsRuntimeExceptionWhenChained() {
        Assert.assertThrows(RuntimeException.class, () -> new SingleReplicaMovementStrategy().chain(new BaseReplicaMovementStrategy()));
    }

    private List<ReplicaPlacementInfo> replicaSet(int... brokerIds) {
        List<ReplicaPlacementInfo> replicaSet = new ArrayList<>();

        for (int brokerId : brokerIds) {
            replicaSet.add(new ReplicaPlacementInfo(brokerId));
        }

        return replicaSet;
    }
}
