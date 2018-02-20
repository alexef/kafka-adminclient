import copy

import logging
from kafka.client_async import KafkaClient
from kafka.errors import KafkaConfigurationError
from kafka.protocol.admin import (
    ListGroupsRequest, DescribeGroupsRequest, CreateTopicsRequest, DeleteTopicsRequest, CreatePartitionsRequest,
)
from kafka.protocol.commit import OffsetFetchRequest
from kafka.protocol.offset import OffsetRequest
from kafka.protocol.metadata import MetadataRequest

from .group import Group
from .topic import Topic

log = logging.getLogger(__name__)


class AdminClient(object):
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
    }
    DELETE_TIMEOUT = 1000
    CREATE_TIMEOUT = 1000
    ALTER_TIMEOUT = 1000

    def __init__(self, **configs):
        # Only check for extra config keys in top-level class
        extra_configs = set(configs).difference(self.DEFAULT_CONFIG)
        if extra_configs:
            raise KafkaConfigurationError("Unrecognized configs: %s" % extra_configs)

        self.config = copy.copy(self.DEFAULT_CONFIG)
        self.config.update(configs)

        self._client = KafkaClient(**self.config)

    def _get_controller_id(self):
        """Get the cluster controller
        """
        node_id = self._client.least_loaded_node()
        m = MetadataRequest[1](topics=[])
        future = self._client.send(node_id, m)
        self._client.poll(future=future)
        response = future.value
        return response.controller_id

    def brokers(self):
        """ Get all brokers """
        return self._client.cluster.brokers()

    def topics(self, exclude_internal_topics=True):
        """Get all topics the user is authorized to view.

        Returns:
            dict: {topic (str): [PartitionMetadata]}
        """
        cluster = self._client.cluster
        if self._client._metadata_refresh_in_progress and self._client._topics:
            future = cluster.request_update()
            self._client.poll(future=future)
        stash = cluster.need_all_topic_metadata
        cluster.need_all_topic_metadata = True
        future = cluster.request_update()
        self._client.poll(future=future)
        cluster.need_all_topic_metadata = stash

        _topics = []
        # FIXME: this should be part of ClusterMetadata class
        for topic, partitions in cluster._partitions.items():
            if exclude_internal_topics and topic in cluster.internal_topics:
                continue
            _topics.append(Topic(topic, partitions.values()))

        return _topics

    def describe_topic(self, topic):
        """ Get all details about a topic, like current offsets for its partitions

        Returns: Topic or None
        """
        cluster = self._client.cluster
        if topic not in cluster.topics():
            # Refresh metadata
            self.topics()

        if topic in cluster.topics():
            topic = Topic(topic, cluster._partitions[topic].values())
        else:
            return None
        # Describe offsets
        for partition in topic.partitions:
            offsets = self.partition_offset(partition)
            partition.set_offset(offsets)

        return topic

    def partition_offset(self, partition):
        """
        Get the latest offset for a given topic partition
        """
        partition_timestamp = (partition.partition, -1)
        request = OffsetRequest[1](
            replica_id=-1,
            topics=[
                (partition.topic, [partition_timestamp]),
            ]
        )
        future = self._client.send(partition.leader, request)
        self._client.poll(future=future)
        response = future.value
        topic = response.topics[0]
        return topic[-1][0][-1]

    def consumer_groups(self):
        """Get all consumer groups known to the cluster

        Returns:
            dict: {group (str): broker_id (int)}
        """
        groups = {}
        for broker in self._client.cluster.brokers():
            request = ListGroupsRequest[0]()
            future = self._client.send(broker.nodeId, request)
            self._client.poll(future=future)
            response = future.value
            if response:
                for g in response.groups:
                    group = g[0]
                    groups[group] = Group(group, broker.nodeId)
            else:
                log.error("No response for ListGroupsRequest")
        return groups

    def describe_consumer_group(self, group):
        """
        Describe a consumer group

        Returns: Group
        """
        groups = self.consumer_groups()
        if group not in groups:
            return None
        group = groups[group]
        request = DescribeGroupsRequest[-1](
            groups=[group]
        )
        future = self._client.send(group.coordinator_id, request)
        self._client.poll(future=future)
        response = future.value
        group.set_metadata(response.groups[0])
        return group

    def consumer_offset_info(self, consumer):
        """ Fetch and configure the consumed offset information for a given consumer group
        """
        all_topics = self.topics()
        topics_request = [(topic.name, [p.partition for p in topic.partitions]) for topic in all_topics]

        o = OffsetFetchRequest[1](
            consumer_group=consumer.name,
            topics=topics_request
        )
        future = self._client.send(consumer.coordinator_id, o)
        self._client.poll(future=future)
        response = future.value
        for r in response.topics:
            topic_name, offsets = r
            if any((o[1] != -1) for o in offsets):
                topic_info = self.describe_topic(topic_name)
                for o in offsets:
                    if o[1] == -1:
                        continue
                    partition = o[0]
                    offset = o[1]
                    consumer.set_offset(topic_name, partition, offset, topic_info.get_offset(partition))
        return consumer

    def create_topic(self, name, partitions, replication_factor):
        """ Create a new topic
        """
        node_id = self._get_controller_id()
        cc = CreateTopicsRequest[0](
            create_topic_requests=[
                (name, partitions, replication_factor, [], []),
            ],
            timeout=self.CREATE_TIMEOUT,
        )
        future = self._client.send(node_id, cc)
        self._client.poll(future=future)
        response = future.value
        error = response.topic_error_codes and response.topic_error_codes[0] and response.topic_error_codes[0][1]
        if error == 0:
            return True
        else:
            log.error('controler: {} create topic error: {}'.format(node_id, error))
            return False

    def delete_topic(self, name):
        """ Delete a topic """
        node_id = self._get_controller_id()
        d = DeleteTopicsRequest[0](
            topics=[name],
            timeout=self.DELETE_TIMEOUT
        )
        future = self._client.send(node_id, d)
        self._client.poll(future=future)
        response = future.value
        error = response.topic_error_codes and response.topic_error_codes[0] and response.topic_error_codes[0][1]
        if error == 0:
            return True
        else:
            log.error('controler: {} delete topic error: {}'.format(node_id, error))
            return False

    def alter_topic(self, topic_name, partitions):
        """ Add partitions """
        node_id = self._get_controller_id()
        a = CreatePartitionsRequest[0](
            topic_partitions=[
                (
                    topic_name,
                    (partitions, None)
                )
            ],
            timeout=self.ALTER_TIMEOUT,
            validate_only=False,
        )
        future = self._client.send(node_id, a)
        self._client.poll(future=future)
        response = future.value
        error = response.topic_errors and response.topic_errors[0] and response.topic_errors[0][1]
        if error == 0:
            return True
        else:
            log.error('controler: {} alter topic error: {}'.format(node_id, error))
            return False
