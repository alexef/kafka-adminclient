class Partition(object):
    """
    A class describing a partition and its offsets

    Arguments:
        metadata (PartitionMetadata):
        offset (int):
    """

    def __init__(self, metadata, offset=None):
        self.topic = metadata.topic
        self.partition = metadata.partition
        self.leader = metadata.leader
        self._metadata = metadata
        self.offset = offset

    def set_offset(self, offset):
        self.offset = offset

    @property
    def members(self):
        return self._metadata and self._metadata.members
