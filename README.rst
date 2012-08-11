Haystack-Cloudsearch -- A backend for Django-Haystack 2.x
==========================================================

Overview
---------
haystack-cloudsearch is a backend for use with the Django Haystack search system and Amazon Cloudsearch. In particular,
it allows using the haystack infrastructure to define abstracted SearchIndexes that can be fed and updated in structured ways
(management commands, realtime via signals, via queues, etc) based around your existing Django models.

haystack-cloudsearch diverges when it comes to querying due to blended search being particularly bad in the context of Cloudsearch,
a system that doesn't provide rank scores or the methodology used for ranking, and requires separate searching of each SearchIndex.
Because of this, haystack-cloudsearch provides a simple api for doing filtered and faceted search, based on what boto provides, but
with convenience functions to map directly to Django QuerySets.

* more information on Django Haystack (version 2.x) can be found here: `haystacksearch.org <http://haystacksearch.org/>`_.
* more information on Amazon Cloudsearch can be found here: `aws.amazon.com/cloudsearch <http://aws.amazon.com/cloudsearch/>`_.

Requirements
-------------
* Python 2.6+
* Django 1.4+ (TODO: test Django 1.3.x support?)
* Django-Haystack 2.x
* Boto from `https://github.com/emidln/boto.git@cloudsearch` (TODO: update when mainline boto has these patches)
* Amazon AWS account credentials

Installation
-------------

#. Install it by running one of the following commands:

   From inside the repo's root directory::

        python setup.py install

   Or, directly from GitHub::

        pip install -e git+https://github.com/pbs/haystack-cloudsearch.git#egg=haystack_cloudsearch

#. Add the following to your project's settings.py::

    HAYSTACK_CONNECTIONS = {
        'default': {
            'ENGINE': 'haystack.backends.cloudsearch_backend.CloudsearchSearchEngine',
            'AWS_ACCESS_KEY_ID': 'YOUR ACCESS KEY HERE',
            'AWS_SECRET_KEY': 'YOUR SECRET KEY HERE',
            'IP_ADDRESS': 'The IP Address you will be accessing cloudsearch from',
        }
    }

#. Add *haystack* to your project's **INSTALLED_APPS**.

Usage
------
Since blended search isn't very useful with respect to Cloudsearch (you can't rank across SearchDomains), I didn't
implement SearchQuerySet. Instead, I implemented the following::

    def search(index_instance, query_string, **query_options)

    def get_backend(index_instance)`

    def get_queryset(index_instance, results)

*search* provides a thin wrapper around the backend's search providing you with the same information a SearchQuerySet would
receieve, namely a dictionary with keys for hits (integer total number of results), results (list of SearchResult objects),
and facets (dictionary of facet names mapped to lists of value, number tuples).

*search* passes `**query_options` onto boto's search, effectively allowing you the api in *boto.cloudsearch.search*. (Document
this here and submit it to boto for their docs as well)

*get_backend* allows you easy access to the default backend, which has a number of features including:

* *backend.get_searchdomain_name* -- takes an index instance and yields a unicode string representing the SearchDomain
* *backend.boto_conn* -- is the live boto cloudsearch layer 2 object. You can use it to get a reference to the SearchDomain like this::
        
        backend = get_backend(my_index_instance)
        backend.boto_conn.get_domain(backend.get_searchdomain_name(my_index_instance))
 
*get_queryset* wraps the results of a search the 'results' key in the dictionary returned by search() and gives you
a Django QuerySet over those results for the appropriate model.

Todo
-----
* Document all the options on search(), then provide that documentation to boto.cloudsearch.search as well
* Handle processing events more sanely in the underlying boto wrapper and continue sanity here.
* Query the environment for AWS_ACCESS_KEY_ID and AWS_SECRET_KEY before raising ImproperlyConfigured.
* AutoQuery support to Cloudsearch's flavor of Boolean Search.
* Testing against a mock service.
* Implement SearchQuerySet despite it being crippled on cloudsearch
