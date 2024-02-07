from sqlalchemy import create_engine
from sqlalchemy.orm import Session


from partition_registry.orm import PartitionEventsORM
from partition_registry.orm import PartitionsRegistryORM
from partition_registry.orm import SourcesRegistryORM
from partition_registry.orm import ProvidersRegistryORM


engine = create_engine('postgresql+psycopg2://postgres:changeme@localhost/partition_registry', echo=True)
session = Session(engine)


rows = (
    session
    .query(
        # Source columns
        SourcesRegistryORM.name,  # 0
        SourcesRegistryORM.access_key, # 1
        SourcesRegistryORM.owner, # 2
        SourcesRegistryORM.registered_at, # 3
        
        # Provider columns
        ProvidersRegistryORM.name, # 4
        ProvidersRegistryORM.access_key, # 5
        ProvidersRegistryORM.registered_at, # 6
        
        # Partition columns
        PartitionsRegistryORM.start, # 7
        PartitionsRegistryORM.end, # 8
        PartitionsRegistryORM.created_at, # 9
        PartitionEventsORM.event_type, # 10
        PartitionEventsORM.created_at # 11
    )
    .join(PartitionEventsORM, PartitionEventsORM.partition_id == PartitionsRegistryORM.id)
    .join(SourcesRegistryORM, SourcesRegistryORM.id == PartitionsRegistryORM.source_id)
    .join(ProvidersRegistryORM, ProvidersRegistryORM.id == PartitionsRegistryORM.provider_id)
    .filter(SourcesRegistryORM.name == 'wm_emails')
    .all()
)

from partition_registry.data.event import PartitionEvent
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import SimplePartition

events: list[PartitionEvent] = []
for row in rows:
    registered_source = RegisteredSource(
        name=row[0],
        access_token=row[1],
        owner=row[2],
        registered_at=row[3]
    )
    
    registered_provider = RegisteredProvider(
        name=row[4],
        access_token=row[5],
        registered_at=row[6],
    )

    simple_partition = SimplePartition(
        start=row[7],
        end=row[8],
        created_at=row[9]
    )
    events.append(
        PartitionEvent(
            partition=simple_partition,
            source=registered_source,
            provider=registered_provider,
            event_type=row[10],
            created_at=row[11]
        )
    )

from pprint import pprint as pp

pp(events)