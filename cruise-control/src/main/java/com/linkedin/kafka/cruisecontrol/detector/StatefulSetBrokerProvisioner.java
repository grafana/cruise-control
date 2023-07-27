/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetSpec;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.PatchUtils;

import java.io.IOException;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED_WITH_ERROR;

public class StatefulSetBrokerProvisioner extends BasicBrokerProvisioner {
    private static final String NAMESPACE_CONFIG = "kubernetes.namespace";
    private String _namespace;

    private ApiClient _client;

    @Override
    public void configure(Map<String, ?> configs) {
        if (!configs.containsKey(NAMESPACE_CONFIG)) {
            throw new ConfigException(String.format("Missing required config: %s", NAMESPACE_CONFIG));
        }
        _namespace = (String) configs.get(NAMESPACE_CONFIG);

        try {
            _client = ClientBuilder.cluster().build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        super.configure(configs);
    }

    @Override
    protected ProvisionerState addOrRemoveBrokers(ProvisionRecommendation rec) {
        try {
            AppsV1Api api = new AppsV1Api(_client);

            V1StatefulSet existing = api.readNamespacedStatefulSet("kafka", _namespace, "false");
            V1StatefulSetSpec spec = existing.getSpec();
            if (spec == null) {
                return new ProvisionerState(COMPLETED_WITH_ERROR, "Error verifying existing replica count");
            }
            Integer existingNumReplicas = spec.getReplicas();
            if (existingNumReplicas == null) {
                return new ProvisionerState(COMPLETED_WITH_ERROR, "Error verifying existing replica count");
            }

            switch (rec.status()) {
                case UNDECIDED:
                case RIGHT_SIZED:
                    return new ProvisionerState(COMPLETED, "Skipped; no right-sizing action recommended.");
                case OVER_PROVISIONED:
                    return new ProvisionerState(COMPLETED,
                            String.format("Skipped recommendation to remove %d brokers.", rec.numBrokers()));
            }

            Integer targetNumReplicas = existingNumReplicas + rec.numBrokers();

            PatchUtils.PatchCallFunc patch = () -> api.patchNamespacedStatefulSetAsync(
                    "kafka",
                    _namespace,
                    new V1Patch(String.format(
                            "[{\"op\":\"replace\",\"path\":\"/spec/replicas\",\"value\":%d}]", targetNumReplicas)),
                    null, null, null, null, null, null);
            PatchUtils.patch(V1Deployment.class, patch, V1Patch.PATCH_FORMAT_JSON_PATCH);

            return new ProvisionerState(COMPLETED,
                    String.format("Recommendation applied; broker count changed from %d to %d", existingNumReplicas, targetNumReplicas));
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
    }
}
