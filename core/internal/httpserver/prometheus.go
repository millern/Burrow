package httpserver

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/linkedin/Burrow/core/protocol"

	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	consumerTotalLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "burrow_kafka_consumer_lag_total",
			Help: "The sum of all partition current lag values for the group",
		},
		[]string{"cluster", "consumer_group"},
	)

	consumerStatusGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "burrow_kafka_consumer_status",
			Help: "The status of the consumer group. It is calculated from the highest status for the individual partitions. Statuses are an index list from NOTFOUND, OK, WARN, ERR, STOP, STALL, REWIND",
		},
		[]string{"cluster", "consumer_group"},
	)

	consumerPartitionCurrentOffset = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "burrow_kafka_consumer_current_offset",
			Help: "Latest offset that Burrow is storing for this partition",
		},
		[]string{"cluster", "consumer_group", "topic", "partition"},
	)

	consumerPartitionLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "burrow_kafka_consumer_partition_lag",
			Help: "Number of messages the consumer group is behind by for a partition as reported by Burrow",
		},
		[]string{"cluster", "consumer_group", "topic", "partition"},
	)

	topicPartitionOffsetGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "burrow_kafka_topic_partition_offset",
			Help: "Latest offset the topic that Burrow is storing for this partition",
		},
		[]string{"cluster", "topic", "partition"},
	)
)

func (hc *Coordinator) handlePrometheusMetrics() http.HandlerFunc {
	promHandler := promhttp.Handler()

	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		fmt.Printf("=======CALL PROMETHEUS=======\n")
		for _, cluster := range listClusters(hc.App) {
			for _, consumer := range listConsumers(hc.App, cluster) {
				fmt.Printf("[START] consumer group %v", consumer)
				consumerStatus := getFullConsumerStatus(hc.App, cluster, consumer)

				if consumerStatus == nil ||
					consumerStatus.Status == protocol.StatusNotFound {
					if consumerStatus != nil {
						fmt.Printf("[debug] skipping consumer group - %v (status %v, totalLag %v, complete %v)\n", consumer, consumerStatus.Status, consumerStatus.TotalLag, consumerStatus.Complete)
					}
					continue
				}

				if consumerStatus.Complete < 1.0 {
					fmt.Printf("[debug] incomplete consumer group - %v (status %v, totalLag %v, complete %v)\n", consumer, consumerStatus.Status, consumerStatus.TotalLag, consumerStatus.Complete)
				}

				labels := map[string]string{
					"cluster":        cluster,
					"consumer_group": consumer,
				}

				consumerTotalLagGauge.With(labels).Set(float64(consumerStatus.TotalLag))
				consumerStatusGauge.With(labels).Set(float64(consumerStatus.Status))

				for _, partition := range consumerStatus.Partitions {
					if partition.Complete < 1.0 {
						fmt.Printf("[debug] skipping partition - [%v:%v:%v] (status %v, complete %v)\n", consumer, partition.Topic, partition, consumerStatus.Status, consumerStatus.Complete)
						continue
					}

					labels := map[string]string{
						"cluster":        cluster,
						"consumer_group": consumer,
						"topic":          partition.Topic,
						"partition":      strconv.FormatInt(int64(partition.Partition), 10),
					}

					consumerPartitionCurrentOffset.With(labels).Set(float64(partition.End.Offset))
					consumerPartitionLagGauge.With(labels).Set(float64(partition.CurrentLag))
				}
			}

			// Topics
			for _, topic := range listTopics(hc.App, cluster) {
				for partitionNumber, offset := range getTopicDetail(hc.App, cluster, topic) {
					topicPartitionOffsetGauge.With(map[string]string{
						"cluster":   cluster,
						"topic":     topic,
						"partition": strconv.FormatInt(int64(partitionNumber), 10),
					}).Set(float64(offset))
				}
			}
		}

		promHandler.ServeHTTP(resp, req)
		fmt.Printf("=======END CALL PROMETHEUS=======\n")
	})
}

func listClusters(app *protocol.ApplicationContext) []string {
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchClusters,
		Reply:       make(chan interface{}),
	}
	app.StorageChannel <- request
	response := <-request.Reply
	if response == nil {
		return []string{}
	}

	return response.([]string)
}

func listConsumers(app *protocol.ApplicationContext, cluster string) []string {
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchConsumers,
		Cluster:     cluster,
		Reply:       make(chan interface{}),
	}
	app.StorageChannel <- request
	response := <-request.Reply
	if response == nil {
		return []string{}
	}

	return response.([]string)
}

func getFullConsumerStatus(app *protocol.ApplicationContext, cluster, consumer string) *protocol.ConsumerGroupStatus {
	request := &protocol.EvaluatorRequest{
		Cluster: cluster,
		Group:   consumer,
		ShowAll: true,
		Reply:   make(chan *protocol.ConsumerGroupStatus),
	}
	app.EvaluatorChannel <- request
	response := <-request.Reply
	return response
}

func listTopics(app *protocol.ApplicationContext, cluster string) []string {
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopics,
		Cluster:     cluster,
		Reply:       make(chan interface{}),
	}
	app.StorageChannel <- request
	response := <-request.Reply
	if response == nil {
		return []string{}
	}

	return response.([]string)
}

func getTopicDetail(app *protocol.ApplicationContext, cluster, topic string) []int64 {
	request := &protocol.StorageRequest{
		RequestType: protocol.StorageFetchTopic,
		Cluster:     cluster,
		Topic:       topic,
		Reply:       make(chan interface{}),
	}
	app.StorageChannel <- request
	response := <-request.Reply
	if response == nil {
		return []int64{}
	}

	return response.([]int64)
}
