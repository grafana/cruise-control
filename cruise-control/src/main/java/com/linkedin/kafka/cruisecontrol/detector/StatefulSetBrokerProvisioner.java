/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.detector;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.analyzer.ProvisionRecommendation;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.client.dsl.base.PatchContext;
import io.fabric8.kubernetes.client.dsl.base.PatchType;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED;
import static com.linkedin.kafka.cruisecontrol.detector.ProvisionerState.State.COMPLETED_WITH_ERROR;

public class StatefulSetBrokerProvisioner extends BasicBrokerProvisioner {
    private static final String NAMESPACE_CONFIG = "kubernetes.namespace";
    private String _namespace;
    private static final Logger LOG = LoggerFactory.getLogger(StatefulSetBrokerProvisioner.class);

    @Override
    public void configure(Map<String, ?> configs) {
        if (!configs.containsKey(NAMESPACE_CONFIG)) {
            throw new ConfigException(String.format("Missing required config: %s", NAMESPACE_CONFIG));
        }
        _namespace = (String) configs.get(NAMESPACE_CONFIG);

        super.configure(configs);
    }

    @Override
    protected ProvisionerState addOrRemoveBrokers(ProvisionRecommendation rec) {

        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            Integer currentNumReplicas = client
                    .apps()
                    .statefulSets()
                    .inNamespace(_namespace)
                    .withName("kafka")
                    .get()
                    .getSpec()
                    .getReplicas();
            if (currentNumReplicas == null) {
                return new ProvisionerState(COMPLETED_WITH_ERROR, "Error verifying existing replica count");
            }

            Integer targetNumReplicas;
            switch (rec.status()) {
                case UNDECIDED:
                case RIGHT_SIZED:
                    return new ProvisionerState(COMPLETED, "Skipped; no right-sizing action recommended.");
                case OVER_PROVISIONED:
                    targetNumReplicas = currentNumReplicas - rec.numBrokers();
                    break;
                case UNDER_PROVISIONED:
                    targetNumReplicas = currentNumReplicas + rec.numBrokers();
                    break;
                default:
                    return new ProvisionerState(COMPLETED_WITH_ERROR,
                            String.format("Error applying recommendation; unknown status %s", rec.status()));
            }

            if (targetNumReplicas < 0) {
                return new ProvisionerState(COMPLETED_WITH_ERROR,
                        String.format("Error applying recommendation; target replica count %d is less than zero", targetNumReplicas));
            }

            ResourceDefinitionContext scaledObjectCrd = new ResourceDefinitionContext.Builder()
                    .withGroup("keda.sh")
                    .withVersion("v1alpha1")
                    .withKind("ScaledObject")
                    .withPlural("scaledobjects")
                    .withNamespaced(true)
                    .build();

            GenericKubernetesResource kafkaScaledObject = client
                    .genericKubernetesResources(scaledObjectCrd)
                    .inNamespace(_namespace)
                    .withName("kafka")
                    .get();

            GenericKubernetesResource patch = new GenericKubernetesResourceBuilder()
                    .withKind(kafkaScaledObject.getKind())
                    .withNewMetadata()
                    .withName(kafkaScaledObject.getMetadata().getName())
                    .withNamespace(kafkaScaledObject.getMetadata().getNamespace())
                    .withResourceVersion(kafkaScaledObject.getMetadata().getResourceVersion())
                    .withAnnotations(Map.of("autoscaling.keda.sh/paused-replicas", String.valueOf(targetNumReplicas)))
                    .endMetadata()
                    .build();

            client.genericKubernetesResources(scaledObjectCrd)
                    .inNamespace(_namespace)
                    .withName("kafka")
                    .patch(
                            new PatchContext.Builder()
                                    .withPatchType(PatchType.SERVER_SIDE_APPLY)
                                    .withFieldManager("cruise-control")
                                    .withForce(true)
                                    .build(),
                            patch
                    );

            return new ProvisionerState(COMPLETED,
                    String.format("Recommendation applied; broker count changed from %d to %d", currentNumReplicas, targetNumReplicas));
        } catch (Exception e) {
            LOG.error("Error applying recommendation", e);
            return new ProvisionerState(COMPLETED_WITH_ERROR, e.getMessage());
        }
    }
}
