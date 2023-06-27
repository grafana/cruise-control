package com.linkedin.kafka.cruisecontrol.monitor.sampling.prometheus;

import com.linkedin.cruisecontrol.common.config.ConfigException;
import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;

import java.util.*;
import java.util.stream.Collectors;

import static com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType.*;

public class KubernetesPodPrometheusQuerySupplier extends DefaultPrometheusQuerySupplier {
    private static final String CLUSTER_CONFIG = "prometheus.query.cluster";
    private static final String NAMESPACE_CONFIG = "prometheus.query.namespace";

    private static final Set<Label> BROKER_TOPIC_LABELS = Set.of(Label.missing("topic"));
    private static final Label TOPIC_TOPIC_LABEL = Label.exists("topic");
    private static final Label NO_QUANTILE_LABEL = Label.missing("quantile");

    private String _cluster;
    private String _namespace;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        if (!configs.containsKey(CLUSTER_CONFIG)) {
            throw new ConfigException(String.format("Missing required config: %s", CLUSTER_CONFIG));
        }
        _cluster = (String) configs.get(CLUSTER_CONFIG);
        if (!configs.containsKey(NAMESPACE_CONFIG)) {
            throw new ConfigException(String.format("Missing required config: %s", NAMESPACE_CONFIG));
        }
        _namespace = (String) configs.get(NAMESPACE_CONFIG);
    }

    @Override
    protected void buildTypeToQueryMap() {
        // Broker metrics
        _typeToQuery.put(BROKER_CPU_UTIL, buildRateQuery("container_cpu_usage_seconds_total"));
        _typeToQuery.put(ALL_TOPIC_BYTES_IN, buildRateQuery("kafka_server_brokertopicmetrics_bytesinpersec", BROKER_TOPIC_LABELS));
        _typeToQuery.put(ALL_TOPIC_BYTES_OUT, buildRateQuery("kafka_server_brokertopicmetrics_bytesoutpersec", BROKER_TOPIC_LABELS));
        _typeToQuery.put(ALL_TOPIC_REPLICATION_BYTES_IN, buildRateQuery("kafka_server_brokertopicmetrics_replicationbytesinpersec"));
        _typeToQuery.put(ALL_TOPIC_REPLICATION_BYTES_OUT, buildRateQuery("kafka_server_brokertopicmetrics_replicationbytesoutpersec"));
        _typeToQuery.put(ALL_TOPIC_FETCH_REQUEST_RATE, buildRateQuery("kafka_server_brokertopicmetrics_totalfetchrequestspersec", BROKER_TOPIC_LABELS));
        _typeToQuery.put(ALL_TOPIC_PRODUCE_REQUEST_RATE, buildRateQuery("kafka_server_brokertopicmetrics_totalproducerequestspersec", BROKER_TOPIC_LABELS));
        _typeToQuery.put(ALL_TOPIC_MESSAGES_IN_PER_SEC, buildRateQuery("kafka_server_brokertopicmetrics_messagesinpersec", BROKER_TOPIC_LABELS));
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_RATE, buildRateQuery("kafka_network_requestmetrics_requestspersec", Set.of(Label.of("request", "Produce"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_RATE, buildRateQuery("kafka_network_requestmetrics_requestspersec", Set.of(Label.of("request", "FetchConsumer"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_RATE, buildRateQuery("kafka_network_requestmetrics_requestspersec", Set.of(Label.of("request", "FetchFollower"))));
        _typeToQuery.put(BROKER_REQUEST_QUEUE_SIZE, buildQuery("kafka_network_requestchannel_requestqueuesize"));
        _typeToQuery.put(BROKER_RESPONSE_QUEUE_SIZE, buildQuery("kafka_network_requestchannel_responsequeuesize", Set.of(Label.missing("processor"))));
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_PRODUCE_REQUEST_QUEUE_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_REQUEST_QUEUE_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_requestqueuetimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_PRODUCE_LOCAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_LOCAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_LOCAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_localtimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_PRODUCE_TOTAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "Produce"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_CONSUMER_FETCH_TOTAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchConsumer"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MAX, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_MEAN, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_50TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_FOLLOWER_FETCH_TOTAL_TIME_MS_999TH, buildQuery("kafka_network_requestmetrics_totaltimems", Set.of(Label.of("request", "FetchFollower"), Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_LOG_FLUSH_RATE, buildRateQuery("kafka_log_logflushstats_logflushrateandtimems", Set.of(NO_QUANTILE_LABEL)));
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_MAX, buildQuery("kafka_log_logflushstats_logflushrateandtimems", Set.of(Label.of("quantile", "Max"))));
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_MEAN, buildQuery("kafka_log_logflushstats_logflushrateandtimems", Set.of(Label.of("quantile", "Mean"))));
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_50TH, buildQuery("kafka_log_logflushstats_logflushrateandtimems", Set.of(Label.of("quantile", "0.50"))));
        _typeToQuery.put(BROKER_LOG_FLUSH_TIME_MS_999TH, buildQuery("kafka_log_logflushstats_logflushrateandtimems", Set.of(Label.of("quantile", "0.999"))));
        _typeToQuery.put(BROKER_REQUEST_HANDLER_AVG_IDLE_PERCENT, buildQuery("kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_total"));

        // Topic metrics
        _typeToQuery.put(TOPIC_BYTES_IN, buildRateQuery("kafka_server_brokertopicmetrics_bytesinpersec", Set.of(TOPIC_TOPIC_LABEL), Set.of("topic")));
        _typeToQuery.put(TOPIC_BYTES_OUT, buildRateQuery("kafka_server_brokertopicmetrics_bytesoutpersec", Set.of(TOPIC_TOPIC_LABEL), Set.of("topic")));
        // Note: The following two metrics aren't outputted by Kafka, but they're also not being used by Cruise Control.
        _typeToQuery.put(TOPIC_REPLICATION_BYTES_IN, buildRateQuery("kafka_server_brokertopicmetrics_replicationbytesinpersec", Set.of(Label.exists("topic"))));
        _typeToQuery.put(TOPIC_REPLICATION_BYTES_OUT, buildRateQuery("kafka_server_brokertopicmetrics_replicationbytesoutpersec", Set.of(Label.exists("topic"))));
        _typeToQuery.put(TOPIC_FETCH_REQUEST_RATE, buildRateQuery("kafka_server_brokertopicmetrics_totalfetchrequestspersec", Set.of(TOPIC_TOPIC_LABEL, NO_QUANTILE_LABEL), Set.of("topic")));
        _typeToQuery.put(TOPIC_PRODUCE_REQUEST_RATE, buildRateQuery("kafka_server_brokertopicmetrics_totalproducerequestspersec", Set.of(TOPIC_TOPIC_LABEL, NO_QUANTILE_LABEL), Set.of("topic")));
        _typeToQuery.put(TOPIC_MESSAGES_IN_PER_SEC, buildRateQuery("kafka_server_brokertopicmetrics_messagesinpersec", Set.of(TOPIC_TOPIC_LABEL, NO_QUANTILE_LABEL), Set.of("topic")));

        // Partition metrics
        _typeToQuery.put(PARTITION_SIZE, buildQuery("kafka_log_log_size", Set.of(Label.exists("topic"), Label.exists("partition")), Set.of("topic", "partition")));

        addInstanceLabelReplaceToAllQueries();
   }

   private void addInstanceLabelReplaceToAllQueries() {
        for (Map.Entry<RawMetricType, String> entry : _typeToQuery.entrySet()) {
            String updated = String.format("label_replace(%s, \"instance\", \"$1.kafka-headless.%s.%s.local:9092\", \"pod\", \"(.+)\")", entry.getValue(), _namespace, _cluster);
            _typeToQuery.put(entry.getKey(), updated);
        }
   }

    private String buildQuery(String metric) {
        return buildQuery(metric, Collections.emptySet());
    }

    private String buildQuery(String metric, Set<Label> additionalLabels) {
        return buildQuery(metric, additionalLabels, Collections.emptySet());
    }

    private String buildQuery(String metric, Set<Label> additionalLabels, Set<String> additionalGroupBys) {
        return String.format("sum(%s{%s}) by (%s)", metric, buildLabels(additionalLabels), buildGroupBys(additionalGroupBys));
    }

   private String buildRateQuery(String metric) {
        return buildRateQuery(metric, Collections.emptySet());
   }

    private String buildRateQuery(String metric, Set<Label> additionalLabels) {
        return buildRateQuery(metric, additionalLabels, Collections.emptySet());
    }

    private String buildRateQuery(String metric, Set<Label> additionalLabels, Set<String> additionalGroupBys) {
        return String.format("sum(rate(%s{%s}[%dm])) by (%s)", metric, buildLabels(additionalLabels), getBrokerCpuUtilQueryMinutes(), buildGroupBys(additionalGroupBys));
    }

    private String buildGroupBys(Set<String> additionalGroupBys) {
        Set<String> groupBys = new HashSet<>();
        groupBys.add("pod");
        groupBys.addAll(additionalGroupBys);

        return String.join(",", groupBys);
    }

    private String buildLabels(Set<Label> additionalLabels) {
        Set<Label> labels = new HashSet<>();
        labels.add(new Label("namespace", _namespace));
        labels.add(new Label("container", "kafka"));
        labels.addAll(additionalLabels);

        return labels.stream().map(Label::build).collect(Collectors.joining(","));
    }

    private static final class Label {
        private final String key;
        private final String value;
        private final String op;

        private static Label of(String key, String value) {
            return new Label(key, value);
        }

        private static Label exists(String key) {
            return new Label(key, "", "!=");
        }

        private static Label missing(String key) {
            return new Label(key, "");
        }

        private Label(String key, String value) {
            this(key, value, "=");
        }

        private Label(String key, String value, String op) {
            this.key = key;
            this.value = value;
            this.op = op;
        }

        private String build() {
            return String.format("%s%s%s", key, op, value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Label label = (Label) o;
            return Objects.equals(key, label.key) && Objects.equals(value, label.value) && Objects.equals(op, label.op);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, op);
        }
    }
}
