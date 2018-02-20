class Group(object):
    """
    A class describing a consumer group

    Arguments:
        name (str): the consumer group name
        coordinator_id (int): the id of the coordinator broker
    """

    def __init__(self, name, coordinator_id):
        self.name = name
        self.coordinator_id = coordinator_id
        self._metadata = None
        self._offsets = {}

    def set_metadata(self, metadata):
        """ Set properties from DescribeGroups command """
        self._metadata = metadata

    def set_offset(self, topic, partition, consumed_offset, offset):
        """ Set the offset from a OffsetFetchResponse """
        print(topic, partition, consumed_offset, offset)
        self._offsets.setdefault(topic, {})
        self._offsets[topic].setdefault(partition, {})
        self._offsets[topic][partition] = {
            'consumed': consumed_offset,
            'offset': offset,
        }

    @property
    def offsets(self):
        return self._offsets
