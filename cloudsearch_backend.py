import logging
import time


from django.core.exceptions import ImproperlyConfigured
from django.db.models.loading import get_model
from django.utils import simplejson

import haystack
from haystack.backends import BaseEngine, BaseSearchBackend, BaseSearchQuery
from haystack.exceptions import MissingDependency
from haystack.models import SearchResult
from haystack.utils import get_identifier

from django.contrib.contenttypes.models import ContentType

from haystack_cloudsearch.utils import (ID, DJANGO_CT, DJANGO_ID,
                                        gen_version,
                                        botobool)
try:
    import boto
except ImportError:
    raise MissingDependency("The 'cloudsearch' backend requires the installation of 'boto'. Please refer to the documentation.")


class CloudsearchSearchBackend(BaseSearchBackend):

    def __init__(self, connection_alias, **connection_options):
        super(CloudsearchSearchBackend, self).__init__(connection_alias, **connection_options)

        if not 'AWS_ACCESS_KEY_ID' in connection_options:
            raise ImproperlyConfigured("You must specify a 'AWS_ACCESS_KEY_ID' in your settings for connection '%s'." % connection_alias)

        if not 'AWS_SECRET_KEY' in connection_options:
            raise ImproperlyConfigured("You must specify a 'AWS_SECRET_KEY' in your settings for connection '%s'." % connection_alias)

        # Allow overrides for the SearchDomain prefix
        self.search_domain_prefix = connection_options.get('SEARCH_DOMAIN_PREFIX', 'haystack')

        self.ip_address = connection_options.get('IP_ADDRESS')
        if self.ip_address is None:
            raise ImproperlyConfigured("You must specify IP_ADDRESS in your settings for connection '%s'." % connection_alias)

        self.boto_conn = boto.connect_cloudsearch(connection_options['AWS_ACCESS_KEY_ID'], connection_options['AWS_SECRET_KEY'])
        # this will become a standard haystack logger down the line
        self.log = logging.getLogger('haystack-cloudsearch')
        self.setup_complete = False

    def enable_index_access(self, index, ip_address):
        """ given an index and an ip_address to enable, enable searching and document services """
        return self.enable_domain_access(self.get_searchdomain_name(index), ip_address)

    def enable_domain_access(self, search_domain, ip_address):
        """ takes the cloudsearch search_domain name  and an ip_address to enable searching and doc services for
        """
        policy = self.boto_conn.get_domain(search_domain).get_access_policies()
        r0 = policy.allow_search_ip(ip_address)
        r1 = policy.allow_doc_ip(ip_address)
        return r0, r1

    def get_searchdomain_name(self, index):
        """ given a SearchIndex, calculate the name for the CloudSearch SearchDomain """
        model = index.get_model()
        ct = ContentType.objects.get_for_model(model)
        return "%s-%s-%s" % tuple(map(lambda x: x.lower(), (self.search_domain_prefix, ct.app_label, unicode(index.__class__.__name__).strip('_'))))

    def get_field_type(self, field):
        """ maps field type classes to cloudsearch field types; raises KeyError if field is unmappable """
        d = {
                'CharField': u'text',
                'FacetCharField': u'text',
                'UnsignedIntegerField': u'uint',
                'LiteralField': u'literal',
                'FacetLiteralField': u'literal',
            }
        return d[field.__class__.__name__]

    def setup(self):
        """ create a cloudsearch schema based on haystack SearchIndexes
            if the haystack models don't match what exists in cloudsearch
        """
        haystack_conn = haystack.connections[self.connection_alias]
        unified_index = haystack_conn.get_unified_index()

        for index in unified_index.collect_indexes():
            search_domain_name = self.get_searchdomain_name(index)
            domain = self.boto_conn.get_domain(search_domain_name)
            should_build_schema = False
            if domain is None:
                domain = self.boto_conn.create_domain(search_domain_name)
                self.setup_complete = False
                should_build_schema = True
            description = self.boto_conn.layer1.describe_index_fields(search_domain_name)
            ideal_schema = self.build_schema(index.fields)
            if not should_build_schema:
                # load the schema as a python data type to compare to the idealized schema
                schema = simplejson.loads(simplejson.dumps([d['options'] for d in description]))
                key = lambda x: x[u'index_field_name']
                if [x for x in sorted(schema, key=key)] != [x for x in sorted(ideal_schema, key=key)]:
                    self.setup_complete = False
                    should_build_schema = True

            # This currently only will handle the create use case
            if should_build_schema and not self.setup_complete:
                for field in ideal_schema:
                    field_type = field[u'index_field_type']

                    args = {'domain_name': search_domain_name,
                            'field_name': field[u'index_field_name'],
                            'field_type': field_type}

                    if field_type == 'uint':
                        default = field[u'u_int_options'][u'default_value']

                    elif field_type == 'text':
                        default = field[u'text_options'][u'default_value']
                        args['facet'] = field[u'text_options'][u'facet_enabled']
                        args['result'] = field[u'text_options'][u'result_enabled']

                    elif field_type == 'literal':
                        default = field[u'literal_options'][u'default_value']
                        args['facet'] = field[u'literal_options'][u'facet_enabled']
                        args['result'] = field[u'literal_options'][u'result_enabled']
                        args['searchable'] = field[u'literal_options']['search_enabled']

                    if default:
                        args['default'] = default

                    self.boto_conn.layer1.define_index_field(**args)

        self.setup_complete = True  # should be True when finished

    def build_schema(self, fields):
        """ return a dictionary describing the schema """

        results = []
        for name, field in fields.iteritems():
            d = {}
            default_value = (u'%s' % (field._default,)) if field.has_default() else {}
            try:
                field_type = self.get_field_type(field)
            except KeyError:
                # This needs to be a real exception
                raise Exception('CloudsearchSearchBackend only supports CharField, UnsignedIntegerField, and LiteralField.')
            d[u'index_field_name'] = unicode(field.index_fieldname)
            d[u'index_field_type'] = field_type
            options = {u'default_value': default_value}
            if field_type == u'uint':
                d[u'u_int_options'] = options
            elif field_type == u'text':
                options[u'facet_enabled'] = botobool(field.faceted)
                options[u'result_enabled'] = botobool(field.stored)
                d[u'text_options'] = options
            elif field_type == u'literal':
                options[u'facet_enabled'] = botobool(field.faceted)
                options[u'result_enabled'] = botobool(field.stored)
                options[u'search_enabled'] = botobool(field.indexed)
                d[u'literal_options'] = options
            if field.stored == field.faceted:
                raise Exception("Fields must either be faceted or stored, not both.") # TODO: make this exception named
            results.append(d)

        # haystack expects these to be able to map a result onto a model
        for field in (DJANGO_ID, DJANGO_CT, ID):
            results.append({
                u'index_field_name': u'%s' % (field,),
                u'index_field_type': u'literal',
                u'literal_options': {
                    u'default_value': {},
                    u'facet_enabled': 'false',
                    u'result_enabled': 'true',
                    u'search_enabled': 'false'}})
        return results

    def get_index_for_obj(self, obj):
        """ inefficiently resolves obj into an index that obj is part of. this could
            return unexpected results if you have more than one index on a given model...

            returns None on failure
        """
        return self.get_model_to_index_map().get(obj.__class__.__name__, None)

    def get_model_to_index_map(self):
        """ returns a dict mapping django model class names to SearchIndexes

            WARNING!!!!
            This can have adverse results if you have more than one SearchIndex mapped
            to the same model...

            This bakes in the same braindead assumption that the rest of haystack makes
            about one ORM Model per Haystack SearchIndex. This will need to change
            when the assumption is lifted.
        """
        haystack_conn = haystack.connections[self.connection_alias]
        unified_index = haystack_conn.get_unified_index()
        return dict((index.get_model().__class__.__name__, index)
                for index in unified_index.collect_indexes())

    def update(self, index, iterable):
        if not self.setup_complete:
            try:
                self.setup()
            # we need to map which exceptions are possible here and handle them appropriately
            except Exception, e:
                if not self.silently_fail:
                    raise
                self.log.error(u'Failed to add documents to Cloudsearch')
                name = getattr(e, '__name__', e.__class__.__name__)
                self.log.error(u'%s while setting up index' % name, exc_info=True,
                        extra={'data': {'index': index}})
                return

        doc_service = self.boto_conn.get_domain(self.get_searchdomain_name(index)).get_document_service()

        prepped_objs = []
        for obj in iterable:
            try:
                prepped_objs.append(index.full_prepare(obj))
            # we need to map which exceptions are possible here and handle them appropriately
            except Exception, e:
                if not self.silently_fail:
                    raise

                name = getattr(e, '__name__', e.__class__.__name__)
                self.log.error(u'%s while preparing object for update' % name, exc_info=True, extra={'data': {'index': index, 'object': get_identifier(obj)}})

        # this needs some help in terms of generating an id
        for obj in prepped_objs:
            obj['id'] = obj['id'].replace('.', '__')
            doc_service.add(obj['id'], gen_version(obj), obj)
        # this can fail if the upload is too large;
        # there should be some error handling around this
        doc_service.commit()

    def remove(self, obj_or_string):
        """ accepts a haystack id such as APP_LABEL.MODEL.PK
            OR a model instance
        """
        if isinstance(obj_or_string, basestring):
            app_label, model_name, pk = obj_or_string.split('.')
            obj_id = u"%s__%s__%s" % (app_label, model_name, pk)
            index = get_model(app_label, model_name)
        else:
            obj_id = u"%s__%s__%s" % (obj_or_string._meta.app_label, obj_or_string._meta.module_name, obj_or_string._get_pk_value())
            index = self.get_index_for_obj(obj_or_string)

        doc_service = self.boto_conn.get_domain(self.get_searchdomain_name(index)).get_document_service()
        doc_service.delete(obj_id, gen_version())
        doc_service.commit()

    def index_event(self, index):
        """ cause reindexing of a particular index """
        return self.boto_conn.layer1.index_documents(self.get_searchdomain_name(index))

    def clear(self, models=None, commit=True, block=True, domains=None, indexes=None):
        """ clear SearchDomains by model, index, or everything """
        # the implementation here just deletes the domain, recreates it, then reloads the schema
        # commit is basically ignored
        domains = domains or []
        if models is not None:
            m = self.get_model_to_index_map()
            for model in models:
                domains.append(self.get_searchdomain_name(m[model.__class__.__name__]))

        if indexes is not None:
            for i in indexes:
                domains.append(self.get_searchdomain_name(i))

        if models is None and indexes is None and domains is None:
            domains = [x['domain_name'] for x in self.boto_conn.layer1.describe_domains()]

        for d in domains:
            self.boto_conn.layer1.delete_domain(d)

        if block:
            while not self.setup_complete:
                try:
                    self.setup()
                except KeyError:
                    if any(map(lambda x: getattr(x, 'processing', False),
                            map(self.boto_conn.get_domain, domains))):
                        time.sleep(30)
                    else:
                        raise
        else:
            # rebuild schema
            self.setup()


    def search(self, query_string, **kwargs):
        """ Blended search across all SearchIndexes.

            limit_indexes - list of indexes to limit the search to (default: search all registered indexes)
        """

        # by convention, empty query strings return no results
        if len(query_string) == 0:
            return {
                    'results': [],
                    'hits': 0,
                    'facets': {},
            }

        if not self.setup_complete:
            self.setup()

        indexes = kwargs.pop('limit_indexes')
        if indexes is None:
            conn = haystack.connections[self.connection_alias]
            unified_index = conn.get_unified_index()
            indexes = unified_index.collect_indexes()

        results = []
        for index in indexes:
            results.append(self._process_results(self.search_index(index, query_string, **kwargs)))

        total_hits = 0
        facets = {}
        total_results = []
        for r in results:
            # this moves the synthetic scores we already know to be sketchy into the realm of completely meaningless
            total_results.extend(r['results'])
            total_hits += r['hits']
            # this will mangle some results if you have facets from two search domains with the same index field name...
            facets.update(r['facets'])

        return {
            'results': total_results,
            'hits': total_hits,
            'facets': facets,
        }

    def field_names_for_index(self, index):
        return [x['index_field_name'] for x in self.build_schema(index.fields)]

    def internal_field_names(self):
        # this shouldn't be hardcoded and thus is a function for now
        return [u'django_id', u'id', u'djang_ct']

    def search_index(self, index, query_string, **kwargs):
        """ given an index and a boolean query, return raw boto results """
        try:
            return_fields = [kwargs.pop('return_fields')]
            return_fields.extend(self.internal_field_names())
            return_fields = list(set(return_fields))
        except KeyError:
            return_fields = self.field_names_for_index(index)
        search_service = self.boto_conn.get_domain(self.get_searchdomain_name(index)).get_search_service()
        query = search_service.search(bq=query_string, return_fields=return_fields, **kwargs)
        return query

    def _process_results(self, boto_results, result_class=None):
        """ return a dict compatible with SearchQuerySet when given raw boto results
            cloudsearch doesn't really provide a scoring mechanism, so we use reverse
            rank as a score
        """
        results = []
        hits = boto_results.hits
        facets = {}
        if result_class is None:
            result_class = SearchResult

        if hasattr(boto_results, 'facets'):
            facets = {
                    'fields': {},
                    'dates': {},
                    'queries': {},
            }

            for facet_fieldname, individuals in boto_results.facets.items():
                facets['fields'][facet_fieldname] = [(x[u'value'], x[u'count']) for x in individuals[u'constraints']]

        unified_index = haystack.connections[self.connection_alias].get_unified_index()
        indexed_models = unified_index.get_indexed_models()

        # this isn't really a score, just the ranking, but it's the best we get
        # out of cloudsearch
        offset = 0 if boto_results.query.start is None else boto_results.query.start
        for weight, result in enumerate([x['data'] for x in boto_results.docs], offset):
            app_label, model_name = result.get(DJANGO_CT)[0].split('.')
            model = get_model(app_label, model_name)
            additional_fields = {}
            score = hits - weight

            if model and model in indexed_models:
                for key, value in result.items():
                    if len(value):
                        value = value[0]
                    else:
                        value = None
                    index = unified_index.get_index(model)
                    string_key = str(key)

                    if string_key in index.fields and hasattr(index.fields[string_key], 'convert'):
                        additional_fields[string_key] = index.fields[string_key].convert(value)
                    else:
                        additional_fields[string_key] = value

                del(additional_fields[DJANGO_CT])
                del(additional_fields[DJANGO_ID])

                result = result_class(app_label, model_name, result[DJANGO_ID][0], score, **additional_fields)
                results.append(result)

        return {
                'results': results,
                'hits': hits,
                'facets': facets,
        }


class CloudsearchSearchQuery(BaseSearchQuery):
    pass


class CloudsearchSearchEngine(BaseEngine):
    backend = CloudsearchSearchBackend
    query = CloudsearchSearchQuery
