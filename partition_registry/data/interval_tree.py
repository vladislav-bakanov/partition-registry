from typing import List

from partition_registry.data.partition import DesiredPartition
from partition_registry.data.partition import SourcePartition
from partition_registry.data.partition_registry_event import PartitionRegistryEvent


class IntervalTree:
    def __init__(self, desired_parition: DesiredPartition) -> None:
        self.desired_partition = desired_parition
        self.nodes: List[PartitionRegistryEvent] = []

    def limit_node(
        self,
        node: PartitionRegistryEvent,
    ) -> PartitionRegistryEvent:
        """Limit current node by desired partition startpoint and endpoint.

        Args:
            node (PartitionRegistryEvent): node to limit by desired partition

        Returns:
            PartitionRegistryEvent: node with invariants:
            - Node startpoint greater or equal than Desired Partition startpoint
            - Node startpoint less or equal than Desired Partition endpoint
        """
        if node.partition.startpoint < self.desired_partition.startpoint:
            node.partition.startpoint = self.desired_partition.startpoint
        if node.partition.endpoint > self.desired_partition.endpoint:
            node.partition.endpoint = self.desired_partition.endpoint
        return node

    def add_node(
        self,
        node: PartitionRegistryEvent
    ) -> None:
        """Add interval node to the interval Tree.
        After this function invariants:
        - First partition within the self.nodes

        Args:
            event (PartitionRegistryEvent): _description_
        """
        if node.partition.startpoint >= self.desired_partition.endpoint:
            pass
        elif node.partition.endpoint <= self.desired_partition.startpoint:
            pass
        else:
            node = self.limit_node(node)
            if len(self.nodes) == 0:
                self.nodes.append(node)
            else:
                self.nodes = [
                    new_node
                    for existed_node in self.nodes
                    for new_node in self.cut_nodes(node, existed_node)
                ]

    def cut_nodes(
        self,
        node1: PartitionRegistryEvent,
        node2: PartitionRegistryEvent
    ) -> List[PartitionRegistryEvent]:
        """Slash 2 events to list of events with uniqueness by: (node.partition, is_ready)

        Args:
            left_node (PartitionRegistryEvent): event with created_date less than right_node.created_date
            right_node (PartitionRegistryEvent): events with created_date greater or equal than left_node.created_date

        Raises:
            ValueError: in case if some case hasn't been covered

        Returns:
            List[PartitionRegistryEvent]: list of events
            Invariants for the returning events:
            1. First event from the list has startpoint greater or equal than desired_partition.startpoint
            2. Last event from the list has endpoint less than desired_partition.endpoint
            3. Every event sorted by created_at has startpoint greater or equal than endpoint of previous event
            4. From p.1 and p.2 => sum of seconds for result less or equal than total seconds in desired_partition
        """
        left_node = node2 if node1.partition.startpoint > node2.partition.startpoint else node1
        right_node = node1 if node1.partition.startpoint > node2.partition.startpoint else node2

        Ls = left_node.partition.startpoint
        Le = left_node.partition.endpoint
        Rs = right_node.partition.startpoint
        Re = right_node.partition.endpoint

        if Rs >= Le:
            return [left_node, right_node]

        if Rs == Ls and Re == Le:
            if left_node.created_date > right_node.created_date:
                return [left_node]
            return [right_node]

        if Rs == Ls and Re < Le:
            if left_node.created_date > right_node.created_date:
                return [left_node]
            return [
                PartitionRegistryEvent(SourcePartition(Ls, Re, right_node.partition.is_ready), right_node.created_date),
                right_node,
                PartitionRegistryEvent(SourcePartition(Re, Le, left_node.partition.is_ready), left_node.created_date),
            ]

        if Rs == Ls and Re > Le:
            if left_node.created_date > right_node.created_date:
                return [
                    left_node,
                    PartitionRegistryEvent(SourcePartition(Le, Re, right_node.partition.is_ready), right_node.created_date)
                ]

            return [right_node]

        if Rs > Ls and Re == Le:
            if left_node.created_date > right_node.created_date:
                return [left_node]
            return [
                PartitionRegistryEvent(SourcePartition(Ls, Rs, left_node.partition.is_ready), left_node.created_date),
                right_node
            ]

        if Rs > Ls and Re < Le:
            if left_node.created_date > right_node.created_date:
                return [left_node]
            return [
                PartitionRegistryEvent(SourcePartition(Ls, Rs, left_node.partition.is_ready), left_node.created_date),
                right_node,
                PartitionRegistryEvent(SourcePartition(Re, Le, left_node.partition.is_ready), left_node.created_date),
            ]

        if Rs > Ls and Re > Le:
            if left_node.created_date > right_node.created_date:
                return [
                    left_node,
                    PartitionRegistryEvent(SourcePartition(Le, Re, right_node.partition.is_ready), right_node.created_date)
                ]
            return [
                PartitionRegistryEvent(SourcePartition(Ls, Rs, left_node.partition.is_ready), left_node.created_date),
                right_node
            ]
        raise ValueError(f"Unknown case for events:\n- {str(left_node)},\n- {str(right_node)}")

