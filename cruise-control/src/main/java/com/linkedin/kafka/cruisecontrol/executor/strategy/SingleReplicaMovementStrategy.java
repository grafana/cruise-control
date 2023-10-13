/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.executor.strategy;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionProposal;
import com.linkedin.kafka.cruisecontrol.executor.ExecutionTask;
import com.linkedin.kafka.cruisecontrol.model.ReplicaPlacementInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SingleReplicaMovementStrategy extends BaseReplicaMovementStrategy {
    @Override
    public String name() {
        return SingleReplicaMovementStrategy.class.getSimpleName();
    }

    @Override
    protected SortedSet<ExecutionTask> preprocessReplicaMovements(Set<ExecutionTask> tasks) {
        return expandExecutionTasks(tasks);
    }

    @Override
    public ReplicaMovementStrategy chain(ReplicaMovementStrategy strategy) {
        throw new RuntimeException("it is invalid to chain other replica movement strategies to SingleReplicaMovementStrategy");
    }

    /**
     * This strategy does not need to be chained to {@link BaseReplicaMovementStrategy} because it
     * enforces the ordering from it already.
     *
     * @return This unchained replica movement strategy.
     */
    @Override
    public ReplicaMovementStrategy chainBaseReplicaMovementStrategyIfAbsent() {
        return this;
    }

    /**
     * Takes an unordered set of {@link ExecutionTask} and expands any {@link ExecutionTask}
     * that contains multiple replica movements into ordered sets of {@link ExecutionTask} that
     * contain single replica movements.
     * <br>
     * The resulting set of {@link ExecutionTask} must remain ordered in order for the proposed
     * distributions to be enacted by the executor correctly.
     *
     * @param replicaMovementTasks The proposed replica movement tasks.
     * @return The expanded replica movement tasks with only one replica movement per task.
     */
    @VisibleForTesting
    static SortedSet<ExecutionTask> expandExecutionTasks(Set<ExecutionTask> replicaMovementTasks) {
        SortedSet<ExecutionTask> expanded = new TreeSet<>();
        long executionTaskId = 0;

        for (ExecutionTask task : replicaMovementTasks) {
            ExecutionProposal proposal = task.proposal();

            // We sort toAdd and toRemove to put the new and old leaders first (respectively, and if
            // applicable). If there is a leadership movement, we'll move the leader first.
            List<ReplicaPlacementInfo> toAdd = proposal.replicasToAdd().stream()
                    .sorted(ReplicaPlacementInfo.LeaderFirstComparator.of(proposal.newLeader()))
                    .collect(Collectors.toList());
            List<ReplicaPlacementInfo> toRemove = proposal.replicasToRemove().stream()
                    .sorted(ReplicaPlacementInfo.LeaderFirstComparator.of(proposal.oldLeader()))
                    .collect(Collectors.toList());

            if (toAdd.size() <= 1) {
                // There is only one replica movement in this execution task; there is no need to expand it, but
                // we do need to assign it a new execution task id.
                expanded.add(new ExecutionTask(executionTaskId++, task.proposal(), task.type(), task.alertTimeMs()));
                continue;
            }

            // There are multiple replica movements in this execution task; split each replica movement into
            // its own execution proposal.
            int i;
            List<ReplicaPlacementInfo> transitionReplicas = proposal.oldReplicas();
            for (i = 0; i < toAdd.size(); i++) {
                List<ReplicaPlacementInfo> nextReplicas = new ArrayList<>(transitionReplicas);
                if (i < toRemove.size()) {
                    nextReplicas.remove(toRemove.get(i));
                }
                nextReplicas.add(toAdd.get(i));

                if (toAdd.get(i).equals(proposal.newLeader())) {
                    // If the broker we're adding is the new leader, make sure that broker is first in the replica set.
                    int leaderPos = nextReplicas.indexOf(proposal.newLeader());
                    nextReplicas.set(leaderPos, nextReplicas.get(0));
                    nextReplicas.set(0, proposal.newLeader());
                }

                expanded.add(new ExecutionTask(
                        executionTaskId++,
                        new ExecutionProposal(
                                proposal.topicPartition(),
                                proposal.partitionSize(),
                                transitionReplicas.get(0),
                                new ArrayList<>(transitionReplicas),
                                new ArrayList<>(nextReplicas)),
                        task.type(),
                        task.alertTimeMs()));
                transitionReplicas = nextReplicas;
            }

            if (i < toRemove.size()) {
                // Any remaining brokers that need to be dropped from the replica set can be safely done in a
                // single task.
                List<ReplicaPlacementInfo> nextReplicas = new ArrayList<>(transitionReplicas);
                for (; i < toRemove.size(); i++) {
                    nextReplicas.remove(toRemove.get(i));
                }
                expanded.add(new ExecutionTask(
                        executionTaskId++,
                        new ExecutionProposal(
                                proposal.topicPartition(),
                                proposal.partitionSize(),
                                transitionReplicas.get(0),
                                new ArrayList<>(transitionReplicas),
                                new ArrayList<>(nextReplicas)),
                        task.type(),
                        task.alertTimeMs()));
            }
        }

        return expanded;
    }
}
