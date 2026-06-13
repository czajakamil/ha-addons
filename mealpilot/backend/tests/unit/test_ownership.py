import pytest

from app import models
from app.ownership import can_change_owner, can_edit, can_view

pytestmark = pytest.mark.unit


def _user(uid):
    u = models.User(username=f"u{uid}", password_hash="h")
    u.id = uid
    return u


def _member(uid, hid, can_edit=False):
    return models.HouseholdMember(user_id=uid, household_id=hid, can_edit=can_edit)


def _personal_recipe(owner_id, created_by=None):
    return models.Recipe(
        id="r", title="t", created_by=created_by or owner_id,
        owner_user_id=owner_id, owner_household_id=None,
    )


def _household_recipe(hid, created_by):
    return models.Recipe(
        id="r", title="t", created_by=created_by,
        owner_user_id=None, owner_household_id=hid,
    )


# --- can_view --------------------------------------------------------------
def test_personal_resource_visible_only_to_owner():
    r = _personal_recipe(owner_id=1)
    assert can_view(r, _user(1), None) is True
    assert can_view(r, _user(2), 5) is False


def test_household_resource_visible_to_members():
    r = _household_recipe(hid=7, created_by=1)
    assert can_view(r, _user(2), 7) is True   # członek tego household
    assert can_view(r, _user(2), 8) is False  # inny household
    assert can_view(r, _user(2), None) is False  # bez household


# --- can_edit --------------------------------------------------------------
def test_personal_resource_editable_only_by_owner():
    r = _personal_recipe(owner_id=1)
    assert can_edit(r, _user(1), None) is True
    assert can_edit(r, _user(2), _member(2, 7, can_edit=True)) is False


def test_household_creator_can_always_edit():
    r = _household_recipe(hid=7, created_by=3)
    assert can_edit(r, _user(3), _member(3, 7, can_edit=False)) is True


def test_household_member_needs_can_edit_flag():
    r = _household_recipe(hid=7, created_by=1)
    assert can_edit(r, _user(2), _member(2, 7, can_edit=False)) is False
    assert can_edit(r, _user(2), _member(2, 7, can_edit=True)) is True


def test_household_member_of_other_household_cannot_edit():
    r = _household_recipe(hid=7, created_by=1)
    assert can_edit(r, _user(2), _member(2, 99, can_edit=True)) is False


def test_household_resource_non_member_cannot_edit():
    r = _household_recipe(hid=7, created_by=1)
    assert can_edit(r, _user(2), None) is False


# --- can_change_owner ------------------------------------------------------
def test_only_creator_can_change_owner():
    r = _personal_recipe(owner_id=1, created_by=1)
    assert can_change_owner(r, _user(1)) is True
    assert can_change_owner(r, _user(2)) is False
