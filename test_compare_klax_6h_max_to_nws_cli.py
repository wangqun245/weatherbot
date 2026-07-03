from decimal import Decimal

from compare_klax_6h_max_to_nws_cli import (
    instantaneous_temp_f,
    obvious_temperature_error,
)


def test_instantaneous_temp_prefers_consistent_precise_group():
    metar = "KPHL 161454Z 19007KT 10SM FEW010 33/24 A3003 RMK AO2 T03330239"

    assert instantaneous_temp_f(metar) == Decimal("91.94")


def test_instantaneous_temp_rejects_conflicting_body_and_precise_group():
    metar = "KDCA 161452Z 00000KT 10SM SCT033 16/04 A3015 RMK AO2 T04780044"

    assert instantaneous_temp_f(metar) is None
    assert obvious_temperature_error(metar)


def test_instantaneous_temp_rejects_implausible_body_temperature():
    metar = "METAR KBOS 300754Z 14005KT 5SM RA BKN042 71/69 A2998 RMK AO2"

    assert instantaneous_temp_f(metar) is None
    assert obvious_temperature_error(metar)
