from atlasclient import client

import random
import uuid


class ApacheAtlasFacade:
    """Apache Atlas API communication facade."""

    _DATA_TYPES = [
        'int', 'string', 'double'
    ]

    _TABLE_NAMES = [
        'school_info', 'personal_info', 'persons', 'employees', 'companies',
        'store', 'home'
    ]

    _COLUMN_NAMES = [
        'name', 'address', 'city', 'state', 'date_time', 'paragraph', 'randomdata',
        'person', 'credit_card', 'size', 'reason', 'school', 'food', 'location',
        'house', 'price', 'cpf', 'cnpj', 'passport', 'security_number',
        'phone_number', 'bank_account_number', 'ip_address', 'stocks'
    ]

    def __init__(self, connection_args):
        # Initialize the API client.
        self.__apache_atlas = client.Atlas(connection_args['host'], port=connection_args['port'],
                                           username=connection_args['user'],
                                           password=connection_args['pass'])

    def create_metadata(self):
        initial_guid = -5000

        # 'b183c071-0d6e-4e0a-9d60-33151c78a7b3'

        for _ in range(1):

            column_guids = []

            # Create 3 columns
            for _ in range(3):
                initial_guid = initial_guid - 1

                column_data = {
                    'typeName': 'Column',
                    'attributes': {},
                    'guid': initial_guid,
                    'status': 'ACTIVE',
                    'createdBy': 'admin',
                    'updatedBy': 'admin',
                    'createTime': 1589983833857,
                    'updateTime': 1591364168996,
                    'version': 0,
                    'classifications': [
                        {
                            'typeName': 'ETL',
                            'entityGuid': 'cb44a958-026c-48d9-b4f3-4a6e2eab7234',
                            'propagate': True,
                            'validityPeriods': []
                        }
                    ]
                }

                column_data['attributes']['dataType'] = self.get_random_data_type()
                name = self.get_random_column_name()
                column_data['attributes']['name'] = name
                column_data['attributes']['comment'] = name

                new_entity = self.__apache_atlas.entity_post.create(data={
                    'entity': column_data
                })

                column_guid = new_entity['guidAssignments'][str(initial_guid)]
                print('created column: ' + name + ' guid: ' + column_guid)
                column_guids.append(column_guid)

            name = self.get_random_table_name()

            initial_guid = initial_guid - 1

            entity_data = {
                'typeName': 'Table',
                'attributes': {
                    'owner': 'John Doe',
                    'temporary': False,
                    'lastAccessTime': 1589983847369,
                    'columns': [],
                    'viewExpandedText': None,
                    'sd': {
                        'guid': '10db96e7-f73f-4e2b-943e-2107ce24acc7',
                        'typeName': 'StorageDesc'
                    },
                    'tableType': 'External',
                    'createTime': 1589983847369,
                    'db': {
                        'guid': '219b1923-98cc-4b36-a9aa-38e9c513e80b',
                        'typeName': 'DB'
                    },
                    'retention': 1589983847365,
                    'viewOriginalText': None
                },
                'guid': initial_guid,
                'status': 'ACTIVE',
                'createdBy': 'admin',
                'updatedBy': 'admin',
                'createTime': 1589983847369,
                'updateTime': 1591278309499,
                'version': 0,
                'relationshipAttributes': {}
            }

            entity_data['attributes']['name'] = name
            entity_data['attributes']['qualifiedName'] = name
            entity_data['attributes']['description'] = name

            for column_guid in column_guids:
                entity_data['attributes']['columns'].append({
                    'typeName': 'Column',
                    'guid': column_guid
                })

            new_entity = self.__apache_atlas.entity_post.create(data={
                'entity': entity_data
            })

            table_guid = new_entity['guidAssignments'][str(initial_guid)]
            print('created table: ' + name + ' guid: ' + table_guid)

    def export_metadata(self):
        # We store each type in a dict to normalize it later on.
        classifications_dict = {}
        entity_type_dict = {}
        expanded_entity_dict = {}

        for metric in self.__apache_atlas.admin_metrics:
            print('Active entities:')
            print(metric.entity['entityActive'])
            print('')

            print('Deleted entities:')
            print(metric.entity['entityDeleted'])
            print('')

            print('General:')
            print(metric.general)
            print('')

        for typedef in self.__apache_atlas.typedefs:

            # Collect classifications/templates
            for classification_type in typedef.classificationDefs:
                print('Classifications:')
                classification_data = classification_type._data
                print(classification_data)
                print('')
                # Classification name is unique
                classifications_dict[classification_type.name] = {'name': classification_type.name,
                                                                  'guid': classification_type.guid,
                                                                  'data': classification_data}

            for enum_def in typedef.enumDefs:
                enum_def._data
                pass

            # Collect entities
            # Clean up entity dict
            entity_dict = {}
            for entity_type in typedef.entityDefs:
                print('')
                fetched_search_results = []
                print('Entity Type: {}'.format(entity_type.name))
                print(entity_type._data)

                # Entity type name is unique
                entity_type_dict[entity_type.name] = {'name': entity_type.name,
                                                      'data': entity_type._data,
                                                      'superTypes': entity_type.superTypes}

                params = {'typeName': entity_type.name, 'offset': 0, 'limit': 100}
                search_results = self.__apache_atlas.search_dsl(**params)
                # Fetch lazy response
                search_results = [entity for s in search_results for entity in s.entities]

                fetched_search_results.extend(search_results)

                print('type: {}'.format(params['typeName']))
                while search_results:
                    params['offset'] = params['offset'] + params['limit']
                    print('offset: {}, limit: {}'.format(params['offset'], params['limit']))
                    search_results = self.__apache_atlas.search_dsl(**params)
                    # Fetch lazy response
                    search_results = [entity for s in search_results for entity in s.entities]
                    fetched_search_results.extend(search_results)

                print('')
                type_guids = []
                for entity in fetched_search_results:
                    # print('Entity:')
                    # print(entity._data)
                    # print('')
                    guid = entity.guid
                    type_guids.append(guid)
                    entity_dict[guid] = {'guid': guid, 'data': entity._data}
                    # print('')

                if type_guids:
                    bulk_collection = self.__apache_atlas.entity_bulk(guid=type_guids)
                    for collection in bulk_collection:
                        entities = collection.entities_with_relationships(attributes=["database"])
                        for expanded_entity in entities:
                            print('Expanded Entity:')
                            guid = expanded_entity.guid
                            entity_data = expanded_entity._data
                            # ADD classificationNames
                            entity_data['classificationNames'] = \
                                entity_dict[expanded_entity.guid]['data']['classificationNames']
                            print(entity_data)
                            expanded_entity_dict[guid] = {'guid': guid, 'data': entity_data}
                            print('')

        returned_entity = self.__apache_atlas.entity_guid('22fc6dcd-ff53-41ae-878e-526e41d6035f')

        cursor = next(returned_entity.classifications)
        classification_list = cursor._data.get('list')
        if classification_list:
            for classification in classification_list:
                pass

        print('Results:')
        print('Entity types:')
        print(entity_type_dict)
        print('Entities:')
        print(expanded_entity_dict)
        print('Classifications:')
        print(classifications_dict)

        return {
            'entity_types': entity_type_dict,
            'entities': expanded_entity_dict,
            'classifications': classifications_dict
        }

    def delete_metadata(self):
        # We store each type in a dict to normalize it later on.
        entities_guids = []
        self.__apache_atlas.entity_bulk.delete(guid=['472686a8-86e7-49b1-8b83-8bb93e4181c6'])
        # self.__delete_all(entities_guids)

    def __delete_all(self, entities_guids):
        for typedef in self.__apache_atlas.typedefs:

            # Collect entities
            # Clean up entity dict
            for entity_type in typedef.entityDefs:
                print('')
                fetched_search_results = []
                print('Entity Type: {}'.format(entity_type.name))
                print(entity_type._data)

                params = {'typeName': entity_type.name, 'offset': 0, 'limit': 100}
                search_results = self.__apache_atlas.search_dsl(**params)
                # Fetch lazy response
                search_results = [entity for s in search_results for entity in s.entities]

                fetched_search_results.extend(search_results)

                print('type: {}'.format(params['typeName']))
                while search_results:
                    params['offset'] = params['offset'] + params['limit']
                    print('offset: {}, limit: {}'.format(params['offset'], params['limit']))
                    search_results = self.__apache_atlas.search_dsl(**params)
                    # Fetch lazy response
                    search_results = [entity for s in search_results for entity in s.entities]
                    fetched_search_results.extend(search_results)

                print('')
                for entity in fetched_search_results:
                    guid = entity.guid
                    entities_guids.append(guid)

                typedef_dict = {
                    "enumDefs": [],
                    "structDefs": [],
                    "classificationDefs": [],
                    "entityDefs": [entity_type._data]
                }

                try:
                    entity = self.__apache_atlas.entity_guid(entity_type.guid)
                    entity.delete()
                    self.__apache_atlas.typedefs.delete(data=typedef_dict)
                except Exception as err:
                    print(str(err))
        self.__apache_atlas.entity_bulk.delete(guid=entities_guids)

    @classmethod
    def get_random_data_type(cls):
        return random.choice(cls._DATA_TYPES)

    @classmethod
    def get_random_column_name(cls):
        return random.choice(cls._COLUMN_NAMES) + uuid.uuid4().hex[:8]

    @classmethod
    def get_random_table_name(cls):
        return random.choice(cls._TABLE_NAMES) + uuid.uuid4().hex[:8]
