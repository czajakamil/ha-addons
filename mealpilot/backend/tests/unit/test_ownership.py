"""Unit tests for backend/app/ownership.py.

Uses an SQLite in-memory database so that `visible_filter` tests can execute
real queries, while the pure-logic helpers (can_view, can_edit,
can_change_owner) are tested without touching the database.
"""
import uuid
from typing import Optional

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session

from app.db import Base
from app import models
from app import ownership


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        future=True,
    )

    @event.listens_for(eng, "connect")
    def _pragma(conn, _record):
        # SQLite does not enforce CHECK constraints by default in older
        # versions, so we only need foreign keys for referential integrity.
        conn.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)


@pytest.fixture
def db(engine) -> Session:
    """Provide a transactional session that is rolled back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = sessionmaker(bind=connection, autoflush=False, autocommit=False, future=True)()
    yield session
    session.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

_counter = 0


def _unique_name(prefix: str) -> str:
    global _counter
    _counter += 1
    return f"{prefix}_{_counter}"


def make_user(db: Session, username: str, is_admin: bool = False) -> models.User:
    user = models.User(
        username=username,
        password_hash="x",
        role="admin" if is_admin else "user",
    )
    db.add(user)
    db.flush()
    return user


def make_household(db: Session, name: str) -> models.Household:
    hh = models.Household(name=name)
    db.add(hh)
    db.flush()
    return hh


def make_member(
    db: Session,
    user: models.User,
    household: models.Household,
    can_edit: bool = True,
    role: str = "member",
) -> models.HouseholdMember:
    m = models.HouseholdMember(
        user_id=user.id,
        household_id=household.id,
        can_edit=can_edit,
    )
    db.add(m)
    db.flush()
    return m


def make_recipe(
    db: Session,
    creator: models.User,
    owner_user: Optional[models.User] = None,
    owner_household: Optional[models.Household] = None,
) -> models.Recipe:
    """Create a Recipe.  Exactly one of owner_user / owner_household must be set."""
    assert (owner_user is None) != (owner_household is None), (
        "Exactly one of owner_user or owner_household must be provided"
    )
    r = models.Recipe(
        id=str(uuid.uuid4()),
        created_by=creator.id,
        owner_user_id=owner_user.id if owner_user else None,
        owner_household_id=owner_household.id if owner_household else None,
        title=_unique_name("recipe"),
    )
    db.add(r)
    db.flush()
    return r


# ---------------------------------------------------------------------------
# Tests: can_view
# ---------------------------------------------------------------------------

class TestCanView:
    def test_owner_sees_personal_recipe(self, db):
        user = make_user(db, _unique_name("owner"))
        recipe = make_recipe(db, creator=user, owner_user=user)
        assert ownership.can_view(recipe, user, household_id=None) is True

    def test_stranger_cannot_see_personal_recipe(self, db):
        owner = make_user(db, _unique_name("owner"))
        stranger = make_user(db, _unique_name("stranger"))
        recipe = make_recipe(db, creator=owner, owner_user=owner)
        assert ownership.can_view(recipe, stranger, household_id=None) is False

    def test_household_member_sees_household_recipe(self, db):
        owner = make_user(db, _unique_name("owner"))
        member_user = make_user(db, _unique_name("member"))
        hh = make_household(db, _unique_name("hh"))
        make_member(db, member_user, hh)
        recipe = make_recipe(db, creator=owner, owner_household=hh)
        assert ownership.can_view(recipe, member_user, household_id=hh.id) is True

    def test_outsider_cannot_see_household_recipe(self, db):
        owner = make_user(db, _unique_name("owner"))
        outsider = make_user(db, _unique_name("outsider"))
        hh = make_household(db, _unique_name("hh"))
        recipe = make_recipe(db, creator=owner, owner_household=hh)
        # outsider has no household_id
        assert ownership.can_view(recipe, outsider, household_id=None) is False

    def test_outsider_with_different_household_cannot_see(self, db):
        owner = make_user(db, _unique_name("owner"))
        outsider = make_user(db, _unique_name("outsider"))
        hh1 = make_household(db, _unique_name("hh1"))
        hh2 = make_household(db, _unique_name("hh2"))
        recipe = make_recipe(db, creator=owner, owner_household=hh1)
        assert ownership.can_view(recipe, outsider, household_id=hh2.id) is False

    def test_admin_has_no_implicit_access_to_others_personal_recipe(self, db):
        """Admins do NOT get automatic access — can_view has no admin bypass."""
        owner = make_user(db, _unique_name("owner"))
        admin = make_user(db, _unique_name("admin"), is_admin=True)
        recipe = make_recipe(db, creator=owner, owner_user=owner)
        assert ownership.can_view(recipe, admin, household_id=None) is False


# ---------------------------------------------------------------------------
# Tests: can_edit
# ---------------------------------------------------------------------------

class TestCanEdit:
    def test_owner_can_edit_personal_recipe(self, db):
        user = make_user(db, _unique_name("owner"))
        recipe = make_recipe(db, creator=user, owner_user=user)
        assert ownership.can_edit(recipe, user, member=None) is True

    def test_creator_of_household_recipe_can_edit_even_if_can_edit_false(self, db):
        creator = make_user(db, _unique_name("creator"))
        hh = make_household(db, _unique_name("hh"))
        member = make_member(db, creator, hh, can_edit=False)
        recipe = make_recipe(db, creator=creator, owner_household=hh)
        assert ownership.can_edit(recipe, creator, member=member) is True

    def test_member_with_can_edit_true_can_edit_household_recipe(self, db):
        creator = make_user(db, _unique_name("creator"))
        editor = make_user(db, _unique_name("editor"))
        hh = make_household(db, _unique_name("hh"))
        make_member(db, creator, hh, can_edit=True)
        member = make_member(db, editor, hh, can_edit=True)
        recipe = make_recipe(db, creator=creator, owner_household=hh)
        assert ownership.can_edit(recipe, editor, member=member) is True

    def test_member_with_can_edit_false_cannot_edit_household_recipe(self, db):
        creator = make_user(db, _unique_name("creator"))
        viewer = make_user(db, _unique_name("viewer"))
        hh = make_household(db, _unique_name("hh"))
        make_member(db, creator, hh, can_edit=True)
        member = make_member(db, viewer, hh, can_edit=False)
        recipe = make_recipe(db, creator=creator, owner_household=hh)
        assert ownership.can_edit(recipe, viewer, member=member) is False

    def test_no_member_cannot_edit_household_recipe(self, db):
        creator = make_user(db, _unique_name("creator"))
        outsider = make_user(db, _unique_name("outsider"))
        hh = make_household(db, _unique_name("hh"))
        recipe = make_recipe(db, creator=creator, owner_household=hh)
        assert ownership.can_edit(recipe, outsider, member=None) is False

    def test_member_of_different_household_cannot_edit(self, db):
        creator = make_user(db, _unique_name("creator"))
        user2 = make_user(db, _unique_name("user2"))
        hh1 = make_household(db, _unique_name("hh1"))
        hh2 = make_household(db, _unique_name("hh2"))
        make_member(db, creator, hh1, can_edit=True)
        member2 = make_member(db, user2, hh2, can_edit=True)
        recipe = make_recipe(db, creator=creator, owner_household=hh1)
        # member2 belongs to hh2, recipe belongs to hh1
        assert ownership.can_edit(recipe, user2, member=member2) is False


# ---------------------------------------------------------------------------
# Tests: can_change_owner
# ---------------------------------------------------------------------------

class TestCanChangeOwner:
    def test_creator_can_change_owner(self, db):
        user = make_user(db, _unique_name("creator"))
        recipe = make_recipe(db, creator=user, owner_user=user)
        assert ownership.can_change_owner(recipe, user) is True

    def test_other_member_cannot_change_owner(self, db):
        creator = make_user(db, _unique_name("creator"))
        other = make_user(db, _unique_name("other"))
        hh = make_household(db, _unique_name("hh"))
        make_member(db, creator, hh, can_edit=True)
        make_member(db, other, hh, can_edit=True)
        recipe = make_recipe(db, creator=creator, owner_household=hh)
        assert ownership.can_change_owner(recipe, other) is False

    def test_admin_non_creator_cannot_change_owner(self, db):
        """Admin role does not grant can_change_owner; only creator matters."""
        creator = make_user(db, _unique_name("creator"))
        admin = make_user(db, _unique_name("admin"), is_admin=True)
        recipe = make_recipe(db, creator=creator, owner_user=creator)
        assert ownership.can_change_owner(recipe, admin) is False


# ---------------------------------------------------------------------------
# Tests: visible_filter (requires real DB queries)
# ---------------------------------------------------------------------------

class TestVisibleFilter:
    def test_returns_only_personal_recipes_for_user_without_household(self, db):
        user = make_user(db, _unique_name("user"))
        other = make_user(db, _unique_name("other"))
        my_recipe = make_recipe(db, creator=user, owner_user=user)
        _their_recipe = make_recipe(db, creator=other, owner_user=other)

        filt = ownership.visible_filter(models.Recipe, user, household_id=None)
        results = db.query(models.Recipe).filter(filt).all()
        ids = {r.id for r in results}

        assert my_recipe.id in ids
        assert _their_recipe.id not in ids

    def test_returns_personal_and_household_recipes_for_member(self, db):
        user = make_user(db, _unique_name("user"))
        hh = make_household(db, _unique_name("hh"))
        make_member(db, user, hh)
        other_creator = make_user(db, _unique_name("other_creator"))

        personal = make_recipe(db, creator=user, owner_user=user)
        hh_recipe = make_recipe(db, creator=other_creator, owner_household=hh)

        filt = ownership.visible_filter(models.Recipe, user, household_id=hh.id)
        results = db.query(models.Recipe).filter(filt).all()
        ids = {r.id for r in results}

        assert personal.id in ids
        assert hh_recipe.id in ids

    def test_does_not_return_other_users_personal_recipe(self, db):
        user = make_user(db, _unique_name("user"))
        other = make_user(db, _unique_name("other"))
        other_recipe = make_recipe(db, creator=other, owner_user=other)

        filt = ownership.visible_filter(models.Recipe, user, household_id=None)
        results = db.query(models.Recipe).filter(filt).all()
        ids = {r.id for r in results}

        assert other_recipe.id not in ids

    def test_does_not_return_other_households_recipes(self, db):
        user = make_user(db, _unique_name("user"))
        creator2 = make_user(db, _unique_name("creator2"))
        hh1 = make_household(db, _unique_name("hh1"))
        hh2 = make_household(db, _unique_name("hh2"))
        make_member(db, user, hh1)

        hh2_recipe = make_recipe(db, creator=creator2, owner_household=hh2)

        filt = ownership.visible_filter(models.Recipe, user, household_id=hh1.id)
        results = db.query(models.Recipe).filter(filt).all()
        ids = {r.id for r in results}

        assert hh2_recipe.id not in ids
