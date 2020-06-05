import logging

from apache_atlas_util import apache_atlas_facade


class ApacheAtlasProcessor:

    def __init__(self, connection_args):
        self.__apache_atlas_facade = apache_atlas_facade.ApacheAtlasFacade(connection_args)

    def export_metadata(self):
        logging.info('')
        logging.info('===> Export Metadata from Apache Atlas [STARTED]')

        logging.info('')
        logging.info(f'Creating metadata export dictionary...')
        metadata_dict = self.__apache_atlas_facade.export_metadata()
        print(metadata_dict)

        logging.info('')
        logging.info('==== Export Metadata from Apache Atlas [FINISHED] =============')

        return metadata_dict

    def create_metadata(self):
        logging.info('')
        logging.info('===> Create Metadata on Apache Atlas [STARTED]')

        logging.info('')
        logging.info(f'Creating metadata dictionary...')
        self.__apache_atlas_facade.create_metadata()

        logging.info('')
        logging.info('==== Create Metadata from Apache Atlas [FINISHED] =============')
