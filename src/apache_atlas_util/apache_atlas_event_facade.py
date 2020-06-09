from kafka import KafkaConsumer


class ApacheAtlasEventFacade:
    __APACHE_ATLAS_SYNC_TOPIC = 'ATLAS_ENTITIES'

    def __init__(self, connection_args):
        self.__connection_args = connection_args

    def create_event_consumer(self):
        consumer = KafkaConsumer(self.__APACHE_ATLAS_SYNC_TOPIC,
                                 api_version=(0, 10),
                                 consumer_timeout_ms=15000,
                                 bootstrap_servers=self.__connection_args['servers'],
                                 group_id=self.__connection_args['consumer_group_id'])
        return consumer
