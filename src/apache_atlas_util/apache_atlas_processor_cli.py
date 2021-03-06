import argparse
import logging
import sys

from apache_atlas_util import apache_atlas_event_processor
from apache_atlas_util import apache_atlas_processor


class ApacheAtlasProcessorCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)

        subparsers = parser.add_subparsers()

        cls.add_apache_atlas_cmd(subparsers)

        return parser.parse_args(argv)

    @classmethod
    def add_apache_atlas_cmd(cls, subparsers):
        apache_atlas_parser = subparsers.add_parser("apache-atlas",
                                                    help="Apache Atlas commands")

        apache_atlas_subparsers = apache_atlas_parser.add_subparsers()

        cls.add_export_metadata_cmd(apache_atlas_subparsers)
        cls.add_delete_metadata_cmd(apache_atlas_subparsers)
        cls.add_create_metadata_cmd(apache_atlas_subparsers)
        cls.add_process_event_metadata_cmd(apache_atlas_subparsers)

    @classmethod
    def add_export_metadata_cmd(cls, subparsers):
        export_metadata_parser = subparsers.add_parser('export',
                                                       help='Export metadata to dict')
        export_metadata_parser.add_argument('--host',
                                            help='Apache Atlas Host',
                                            required=True)
        export_metadata_parser.add_argument('--port',
                                            help='Apache Atlas Port',
                                            required=True)
        export_metadata_parser.add_argument('--user',
                                            help='Apache Atlas User',
                                            required=True)
        export_metadata_parser.add_argument('--passsword',
                                            help='Apache Atlas Pass',
                                            required=True)
        export_metadata_parser.set_defaults(func=cls.__export_metadata)

    @classmethod
    def add_create_metadata_cmd(cls, subparsers):
        create_metadata_parser = subparsers.add_parser('create',
                                                       help='Create metadata')
        create_metadata_parser.add_argument('--host',
                                            help='Apache Atlas Host',
                                            required=True)
        create_metadata_parser.add_argument('--port',
                                            help='Apache Atlas Port',
                                            required=True)
        create_metadata_parser.add_argument('--user',
                                            help='Apache Atlas User',
                                            required=True)
        create_metadata_parser.add_argument('--passsword',
                                            help='Apache Atlas Pass',
                                            required=True)
        create_metadata_parser.set_defaults(func=cls.__create_metadata)

    @classmethod
    def add_delete_metadata_cmd(cls, subparsers):
        delete_metadata_parser = subparsers.add_parser('delete',
                                                       help='Delete metadata')
        delete_metadata_parser.add_argument('--host',
                                            help='Apache Atlas Host',
                                            required=True)
        delete_metadata_parser.add_argument('--port',
                                            help='Apache Atlas Port',
                                            required=True)
        delete_metadata_parser.add_argument('--user',
                                            help='Apache Atlas User',
                                            required=True)
        delete_metadata_parser.add_argument('--passsword',
                                            help='Apache Atlas Pass',
                                            required=True)
        delete_metadata_parser.set_defaults(func=cls.__delete_metadata)

    @classmethod
    def add_process_event_metadata_cmd(cls, subparsers):
        process_event_metadata_parser = subparsers.add_parser('process-event-metadata',
                                                              help='Process Event Metadata')
        process_event_metadata_parser.add_argument('--servers',
                                                   help='Event bus server list, split by comma',
                                                   required=True)
        process_event_metadata_parser.add_argument('--consumer-group-id',
                                                   help='Consumer Group id used to connect to '
                                                        'ATLAS_ENTITIES topic',
                                                   required=True)
        process_event_metadata_parser.set_defaults(func=cls.__process_event_metadata)

    @classmethod
    def __export_metadata(cls, args):
        apache_atlas_processor.ApacheAtlasProcessor({
            'host': args.host,
            'port': args.port,
            'user': args.user,
            'pass': args.passsword
        }).export_metadata()

    @classmethod
    def __delete_metadata(cls, args):
        apache_atlas_processor.ApacheAtlasProcessor({
            'host': args.host,
            'port': args.port,
            'user': args.user,
            'pass': args.passsword
        }).delete_metadata()

    @classmethod
    def __create_metadata(cls, args):
        apache_atlas_processor.ApacheAtlasProcessor({
            'host': args.host,
            'port': args.port,
            'user': args.user,
            'pass': args.passsword
        }).create_metadata()

    @classmethod
    def __process_event_metadata(cls, args):
        apache_atlas_event_processor.ApacheAtlasEventProcessor({
            'servers': args.servers.split(','),
            'consumer_group_id': args.consumer_group_id
        }).process_event_metadata()


def main():
    argv = sys.argv
    ApacheAtlasProcessorCLI.run(argv[1:] if len(argv) > 0 else argv)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
