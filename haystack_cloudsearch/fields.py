
from haystack.fields import CharField, FacetField, IntegerField, MultiValueField


class LiteralField(CharField):

    def __init__(self, **kwargs):
        if kwargs.get('facet_class') is None:
            kwargs['facet_class'] = FacetLiteralField
        super(CharField, self).__init__(**kwargs)


class FacetLiteralField(FacetField, LiteralField):
    pass


class UnsignedIntegerField(IntegerField):

    def convert(self, value):
        v = super(UnsignedIntegerField, self).convert(value)
        if v is None or v < 0:
            raise TypeError("UnsignedIntegerField does not allow negative integers.")


class MultiValueCharField(MultiValueField):

    def __init__(self, **kwargs):
        if kwargs.get('facet_class') is None:
            kwargs['facet_class'] = FacetMultiValueCharField
        super(MultiValueField, self).__init__(**kwargs)


class FacetMultiValueCharField(FacetField, MultiValueCharField):
    pass


class MultiValueUnsignedIntegerField(MultiValueField):
    field_type = 'integer'


class MultiValueLiteralField(MultiValueCharField):

    def __init__(self, **kwargs):
        if kwargs.get('facet_class') is None:
            kwargs['facet_class'] = FacetMultiValueLiteralField
        super(MultiValueField, self).__init__(**kwargs)


class FacetMultiValueLiteralField(FacetField, MultiValueLiteralField):
    pass
