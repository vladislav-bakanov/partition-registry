# from typing import Set
# import datetime as dt

# from partition_registry.data.source import SimpleSource
# from partition_registry.data.source import RegisteredSource
# from partition_registry.data.partition import SimplePartition

# from partition_registry.data.partition import is_intersected

# from partition_registry.actor.registry import PartitionRegistry
# from partition_registry.actor.registry import SourceRegistry


# def is_partition_comprehensively_covered(
#     desired_partition: SimplePartition,
#     partitions: Set[SimplePartition]
# ) -> bool:
#     """
#     Check whether desired partition can be covered by the specified partitions.
    
#     Params:
#         desired_partition: SimplePartition - simple interval [start - end]
#         partitions - set[SimplePartition] - simple intervals to check whether they cover desired partition

#     Returns: bool
#     """
#     if not partitions:
#         return False

#     list_partitions = list(partitions)
#     # Case when intersected partition start date is greater than start date in desired partition
#     # and due to that fact can't be comprehensively covered
#     if min(list_partitions, key=lambda x: x.start).start > desired_partition.start:
#         return False

#     # Case when intersected partition start date is less than end date in desired partition
#     # and due to that fact can't be comprehensively covered
#     if max(list_partitions, key=lambda x: x.end).end < desired_partition.end:
#         return False

#     list_partitions.sort(key=lambda x: x.start)
#     current_end = list_partitions[0].end
#     location = 1

#     # We return False here only in case when we found a gap between 2 consistent partitions
#     # Examples:
#     # 1. Desired interval: |2000-01-01: 2000-01-04|
#     #    Partitions: |2000-01-01: 2000-01-02|, |2000-01-01: 2000-01-04| - should comprehensively cover interval
#     #
#     # 2. Desired interval: |2000-01-01: 2000-01-04|
#     #    Partitions: |2000-01-01: 2000-01-02|, |2000-01-03: 2000-01-04| - shouldn't comprehensively cover interval
#     #
#     # In terms of Big O we have O(n) complexity here for the worst case
#     while location < len(list_partitions):
#         if current_end < list_partitions[location].start:
#             return False
#         current_end = list_partitions[location].end
#         location += 1

#     return True



# def is_partition_ready(
#     source: str,
#     start: dt.datetime,
#     end: dt.datetime,
#     source_registry: SourceRegistry,
#     partition_registry: PartitionRegistry
# ) -> bool:
#     """Checks whether Source Partition is ready"""
#     simple_partition = SimplePartition(start, end)
#     simple_partition.validate()

#     simple_source = SimpleSource(source)
#     simple_source.validate()

#     match source_registry.find_registered_source(simple_source):
#         case RegisteredSource() as registered_source: ...
#         case _: return False

#     for lp in partition_registry.get_locked_partitions_by_source(registered_source):
#         if is_intersected(simple_partition, lp):
#             return False

#     unlocked_partitions = list(partition_registry.get_unlocked_partitions_by_source(registered_source))
#     simplified_unlocked_partitions = {
#         unlocked_partition
#         for unlocked_partition in partition_registry.simplify_unlocked(unlocked_partitions)
#         if is_intersected(simple_partition, unlocked_partition)
#     }

#     return is_partition_comprehensively_covered(simple_partition, simplified_unlocked_partitions)
