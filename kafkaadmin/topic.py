from .partition import Partition


class Topic(object):
    """
    A class describing a topic and all of its partitions.

    Arguments:
        name (str): topic name
        partitions (list(PartitionMetadata)): list of partition metadata objects
    """

    def __init__(self, name, partitions):
        self.name = name
        self.partitions = [Partition(p) for p in partitions]

    def get_offset(self, partition):
        for p in self.partitions:
            if partition == p.partition:
                return p.offset
        return -1
