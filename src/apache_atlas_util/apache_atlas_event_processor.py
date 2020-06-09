import json
import logging
from time import sleep

from apache_atlas_util import apache_atlas_event_facade


class ApacheAtlasEventProcessor:

    def __init__(self, connection_args):
        self.__apache_atlas_event_facade = apache_atlas_event_facade.ApacheAtlasEventFacade(
            connection_args)

    def process_event_metadata(self):
        logging.info('')
        logging.info('===> Process Event Metadata from Apache Atlas [STARTED]')

        logging.info('')
        logging.info('Creating consumer, will loop processing messages...')

        while True:
            logging.info('Processing...')
            event_consumer = self.__apache_atlas_event_facade.create_event_consumer()

            for msg in event_consumer:
                if msg:
                    print("{}:{}:{}: key={} ".format(msg.topic, msg.partition,
                                                     msg.offset, msg.key))
                    event = json.loads(msg.value)
                    print(event)

            event_consumer.close()
            logging.info('Sleeping...')
            sleep(5)
