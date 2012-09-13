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

haystack-cloudsearch comes with a variety of custom field types to meet Cloudsearch requirements. In addition to the typical
haystack.fields.CharField (and haystack.fields.FacetCharField), you get the following in the haystack_cloudsearch.fields namespace:

* UnsignedIntegerField (faceting is implied due to the way cloudsearch works)
* LiteralField (and FacetLiteralField)
* MultiValueCharField (and FacetMultiValueCharField) (this is basically the same as haystack.fields.MultiValueField)
* MultiValueLiteralField (and FacetMultiValueLiteralField)
* MultiValueUnsignedIntegerField (faceted is implied due to the way cloudsearch works)

Pull requests are welcome. In particular, the tests are still getting up to speed, and it's an open question of how much of
SearchQuerySet is worth implementing to gain features written around Haystack. 

This heavily depends on the excellent boto library. The boto plugin for cloudsearch is still very new and would also appreciate
pull requests.

* more information on Django Haystack (version 2.x) can be found here: `haystacksearch.org <http://haystacksearch.org/>`_.
* more information on Amazon Cloudsearch can be found here: `aws.amazon.com/cloudsearch <http://aws.amazon.com/cloudsearch/>`_.
* more information on Boto can be found here: `github.com/boto/boto <https://github.com/boto/boto/>`_.

Requirements
-------------
* Python 2.7 (TODO: test 2.6, PyPy)
* Django 1.4+ (TODO: test Django 1.3.x support?)
* Django-Haystack 2.x
* Boto from `https://github.com/pbs/boto/tree/cloudsearch` (TODO: update when mainline boto has these patches)
* Amazon AWS account credentials

Installation
-------------

#. Install it by running one of the following commands:

   First, install the modified boto::

        pip install -e git+https://github.com/pbs/boto.git@cloudsearch#egg=boto

   From inside the repo's root directory::

        python setup.py install

   Or, directly from GitHub::

        pip install -e git+https://github.com/pbs/haystack-cloudsearch.git@develop#egg=haystack_cloudsearch

#. Add the following to your project's settings.py::

    HAYSTACK_CONNECTIONS = {
        'default': {
            'ENGINE': 'haystack_cloudsearch.cloudsearch_backend.CloudsearchSearchEngine',
            'AWS_ACCESS_KEY_ID': 'YOUR ACCESS KEY HERE',
            'AWS_SECRET_KEY': 'YOUR SECRET KEY HERE',
            'IP_ADDRESS': 'The IP Address you will be accessing cloudsearch from',
            #'SEARCH_DOMAIN_PREFIX': 'optional string to namespace your search domain with; defaults to haystack'
        }
    }

#. Add *haystack* to your project's **INSTALLED_APPS**.

Usage
------
Cloudsearch-specific fields can be found in haystack_cloudsearch.fields. LiteralField, FacetedLiteralField, and UnsignedIntegerField,
are available for use alongside CharField and FacetedCharField. MultiValue and FacetMultiValue versions are also available.

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

The way to bootstrap the system by hand is like this (in the shell)::

  >>> from myapp.search_indexes import MyIndex
  >>> from haystack_cloudsearch.cloudsearch_utils import get_backend
  >>> i = MyIndex()
  >>> b = get_backend(i)
  >>> b.setup()
  >>> b.enable_index_access(i, b.ip_address)
  >>> b.boto_conn.layer1.index_documents(b.get_searchdomain_name(i))
  >>> def get_domain():
  ...     return b.boto_conn.get_domain(b.get_searchdomain_name(i))
  ...
  >>> import time
  >>> t0 = int(time.time())
  >>> while True:
  ...     if not get_domain().processing:
  ...         print int(time.time()) - t0
  ...         break
  ...     time.sleep(30)
  ...
  >>> b.update(i, i.index_queryset().all())

The update can fail, and there really should be a generalized processing wait utility as well as a utility to
get a domain given an index. This should further be wrapped up to replace the appropriate management commands.

Spinlocks (or, Amazon plz can haz webhookz/queue_service?)
---------------------------------------------------
Cloudsearch requires processing for most administrative changes. These typically take at least 15 minutes to complete. Because of this,
you may encounter spinlocks (logged at the DEBUG level). This ensures that certain actions aren't taken "out of order". For example,
deleting a search domain followed by creating one of the same name (a clear()), will normally result in an "undelete" operation. This
typically isn't intended, and leads to non-obvious schema conflicts. As such, some operations now take a spinlock=True argument, particularly
in the backend. Those that currently don't, should be modified to.

License
--------
Copyright 2012 Public Broadcasting Service

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Todo
-----
* Document all the options on search(), then provide that documentation to boto.cloudsearch.search as well
* Handle processing events more sanely in the underlying boto wrapper and continue sanity here.
* Query the environment for AWS_ACCESS_KEY_ID and AWS_SECRET_KEY before raising ImproperlyConfigured.
* AutoQuery support to Cloudsearch's flavor of Boolean Search.
* Testing against a mock service.
* Implement SearchQuerySet despite it being crippled on cloudsearch
