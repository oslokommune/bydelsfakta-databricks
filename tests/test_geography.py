import pytest

from bydelsfakta.geography import (
    BYDELER,
    DELBYDELER,
    _BYDELER_BY_NAME,
    _DELBYDELER_BY_NAME,
    _DELBYDEL_BY_OLD_ID,
    bydel_by_id,
    bydel_by_name,
    delbydel_by_id,
    delbydel_by_name,
    delbydel_by_old_id,
)


class TestBydelFraId:
    def test_valid_id(self):
        result = bydel_by_id("01")
        assert result == {"id": "01", "name": "Bydel Gamle Oslo"}

    def test_integer_id(self):
        assert bydel_by_id(1) is None

    def test_oslo_i_alt(self):
        assert bydel_by_id("00")["name"] == "Oslo i alt"

    def test_unknown_id(self):
        assert bydel_by_id("42") is None


class TestBydelFraNavn:
    def test_exact_match(self):
        result = bydel_by_name("Bydel Gamle Oslo")
        assert result["id"] == "01"

    def test_prefix_fallback(self):
        result = bydel_by_name("Gamle Oslo")
        assert result["id"] == "01"

    def test_case_insensitive(self):
        result = bydel_by_name("BYDEL SAGENE")
        assert result["id"] == "03"

    def test_strips_whitespace(self):
        result = bydel_by_name("  Bydel Frogner  ")
        assert result["id"] == "05"

    def test_st_hanshaugen_variants(self):
        assert bydel_by_name("Bydel St. Hanshaugen")["id"] == "04"
        assert bydel_by_name("Bydel St.Hanshaugen")["id"] == "04"
        assert bydel_by_name("Bydel St Hanshaugen")["id"] == "04"

    def test_special_names(self):
        assert bydel_by_name("Sentrum")["id"] == "16"
        assert bydel_by_name("Marka")["id"] == "17"
        assert bydel_by_name("Uoppgitt")["id"] == "99"

    def test_not_found(self):
        assert bydel_by_name("Finnes ikke") is None


class TestDelbydelFraNyId:
    def test_four_digit_string(self):
        result = delbydel_by_id("0101")
        assert result["name"] == "Lodalen"
        assert result["bydel_id"] == "01"

    def test_zero_padding(self):
        result = delbydel_by_id(101)
        assert result["id"] == "0101"

    def test_float_input(self):
        result = delbydel_by_id(101.0)
        assert result["id"] == "0101"

    def test_not_found(self):
        assert delbydel_by_id(9998) is None


class TestDelbydelFraNavn:
    def test_valid(self):
        result = delbydel_by_name("Grønland")
        assert result["id"] == "0102"

    def test_case_insensitive(self):
        result = delbydel_by_name("BJØLSEN")
        assert result["id"] == "0303"

    def test_strips_whitespace(self):
        result = delbydel_by_name("  Kampen  ")
        assert result["id"] == "0105"

    def test_not_found(self):
        assert delbydel_by_name("Finnes ikke") is None


class TestDelbydelFraGammelId:
    def test_string_id(self):
        result = delbydel_by_old_id("011")
        assert result["id"] == "0101"

    def test_integer_id(self):
        result = delbydel_by_old_id(11)
        assert result["id"] == "0101"

    def test_not_found(self):
        assert delbydel_by_old_id("000") is None

    def test_special_999(self):
        result = delbydel_by_old_id("999")
        assert result["id"] == "9901"


class TestDataIntegrity:
    def test_all_delbydel_bydel_ids_exist(self):
        for delbydel_id, delbydel in DELBYDELER.items():
            assert delbydel["bydel_id"] in BYDELER, (
                f"Delbydel {delbydel_id} references unknown bydel "
                f"{delbydel['bydel_id']}"
            )

    def test_bydeler_by_name_values_exist(self):
        for name, bydel_id in _BYDELER_BY_NAME.items():
            assert bydel_id in BYDELER, (
                f"Name '{name}' maps to unknown bydel {bydel_id}"
            )

    def test_delbydeler_by_name_values_exist(self):
        for name, delbydel_id in _DELBYDELER_BY_NAME.items():
            assert delbydel_id in DELBYDELER, (
                f"Name '{name}' maps to unknown delbydel {delbydel_id}"
            )

    def test_gammel_id_values_exist(self):
        for old_id, new_id in _DELBYDEL_BY_OLD_ID.items():
            assert new_id in DELBYDELER, (
                f"Old ID '{old_id}' maps to unknown delbydel {new_id}"
            )

    def test_all_15_residential_districts(self):
        residential = [
            k for k in BYDELER if k not in ("00", "16", "17", "99")
        ]
        assert len(residential) == 15

    def test_bydeler_id_matches_key(self):
        for key, bydel in BYDELER.items():
            assert bydel["id"] == key

    def test_delbydeler_id_matches_key(self):
        for key, delbydel in DELBYDELER.items():
            assert delbydel["id"] == key
