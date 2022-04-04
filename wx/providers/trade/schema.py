from marshmallow import Schema, fields, post_load
import wx.models.wx_helpers as wx_helpers


class FloatSanitize(fields.Field):
    @staticmethod
    def _serialize(value, attr, obj, **kwargs):
        if value is None:
            return 'None'
        else:
            return str(value)

    @staticmethod
    def _deserialize(value, attr, obj, **kwargs):
        if value == 'None':
            return None
        else:
            return float(value)


class DateTimeSanitize(fields.Field):
    @staticmethod
    def _serialize(value, attr, obj, **kwargs):
        if value is None:
            return 'None'
        else:
            return str(value.strftime('%Y-%m-%d'))


class VanillaPayOffSchema(Schema):
    type = fields.Str()
    strike = FloatSanitize(attribute='strike')
    notional = FloatSanitize(attribute='notional')
    buysell = fields.Str()
    limit_lc = FloatSanitize(attribute='limit_lc')
    limit_cpty = FloatSanitize(attribute='limit_cpty')

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.VanillaPayoff(data)


class VanillaSubIndexSchema(Schema):
    location = fields.Str()
    index = fields.Str()
    index_threshold = FloatSanitize(attribute='index_threshold')
    index_daily_max = FloatSanitize(attribute='index_daily_max')
    index_daily_min = FloatSanitize(attribute='index_daily_min')
    index_aggregation = fields.Str()
    underlying = fields.Str()
    underlying_unit = fields.Str()
    risk_start = DateTimeSanitize(attribute='risk_start')
    risk_end = DateTimeSanitize(attribute='risk_end')
    weight = FloatSanitize(attribute='weight')
    name = fields.Str()

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.VanillaSubIndex(data)


class LegSchema(Schema):
    vanilla_index = fields.List(fields.Nested(VanillaSubIndexSchema))
    name = fields.Str()
    payoff = fields.Nested(VanillaPayOffSchema)

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.Leg(data)


class VanillaSchema(Schema):
    legs = fields.List(fields.Nested(LegSchema))
    aggregate_limit_lc = FloatSanitize(attribute='aggregate_limit_lc')
    aggregate_limit_cpty = FloatSanitize(attribute='aggregate_limit_cpty')
    aggregate_deductible = FloatSanitize(attribute='aggregate_deductible')
    counterparty = fields.Str()
    risk_region = fields.Str()
    risk_sub_region = fields.Str()
    create_date_time = fields.Str()
    quoted_date_time = fields.Str()
    quoted_y_n = fields.Str()
    traded_y_n = fields.Str()
    traded_date_time = fields.Str()
    deal_number = fields.Str()
    fair_value = FloatSanitize(attribute='fair_value')
    premium = FloatSanitize(attribute='premium')

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.Vanilla(data)

class PriceSubIndexSchema(Schema):
    location = fields.Str()
    type = fields.Str()
    hours = FloatSanitize(attribute='index_threshold')
    index_daily_max = FloatSanitize(attribute='index_daily_max')
    index_daily_min = FloatSanitize(attribute='index_daily_min')
    index_aggregation = fields.Str()
    forward_source = fields.Str()
    forward_id = fields.Str()
    risk_start = DateTimeSanitize(attribute='risk_start')
    risk_end = DateTimeSanitize(attribute='risk_end')
    weight = FloatSanitize(attribute='weight')
    name = fields.Str()

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.PriceSubIndex(data)

class PricePayOffSchema(Schema):
    type = fields.Str()
    strike = FloatSanitize(attribute='strike')
    notional = FloatSanitize(attribute='notional')
    buysell = fields.Str()
    limit_lc = FloatSanitize(attribute='limit_lc')
    limit_cpty = FloatSanitize(attribute='limit_cpty')

    @post_load()
    def create_object(self, data, **kwargs):
        return wx_helpers.PricePayoff(data)