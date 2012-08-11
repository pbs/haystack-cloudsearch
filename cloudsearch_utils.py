
import time

from haystack.constants import ID, DJANGO_CT, DJANGO_ID

### Useful For Querying
def get_backend(index_instance, using=None):
    """ given an index, return the backend, by default using the default connection """
    using = using or 'default'
    return index_instance._get_backend(using=using)


def get_queryset_from_results(results):
    """ given some homogeneous results, return a django queryset for their corresponding objects """
    return results[0].searchindex.index_queryset().filter(pk__in=[x.pk for x in results])


def search(index_instance, query_string, **query_options):
    """ search a SearchIndex instance for query_string with query_options, returns
        a dict with results, facets, and hits

        A particular connection can be specified by passing using in query_options.
    """
    using = query_options.pop('using', 'default')
    return get_backend(index_instance, using=using).search(query_string, limit_indexes=[index_instance], **query_options)

### Collection of backend hacks
ID = unicode(ID)
DJANGO_CT = unicode(DJANGO_CT)
DJANGO_ID = unicode(DJANGO_ID)


def unix_epoch_seconds():
    """ seconds since jan 1, 1970 utc """
    return int(time.time())


def gen_version(instance, default=unix_epoch_seconds):
    """ given a model instance, generate a version for that instance """
    return default()


def django_id_to_cloudsearch(s):
    """ convert haystack ids to legal cloudsearch index field names """
    return s.replace('.', '__')


def cloudsearch_to_django_id(s):
    """ convert cloudsearch index field names to haystack ids """
    return s.replace('__', '.')


def instance_to_dict(obj):
    """ given a model instance, return a dict of field_names to values """
    return dict((f.name, getattr(obj, f)) for f in obj._meta.fields)


def botobool(obj):
    """ returns boto results compatible value """
    return u'false' if not bool(obj) else u'true'
