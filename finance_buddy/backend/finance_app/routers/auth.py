"""Authentication, passkey, and user-management endpoints."""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import json
import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import Response
from fido2 import features as fido2_features
from fido2.server import Fido2Server
from fido2.webauthn import (
    AttestedCredentialData,
    AuthenticationResponse,
    PublicKeyCredentialRpEntity,
    PublicKeyCredentialUserEntity,
    ResidentKeyRequirement,
    UserVerificationRequirement,
)
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db_session
from ..dependencies import (
    _assign_existing_data_to_user,
    _get_bearer_token,
    _require_admin,
    _require_session,
)
from ..helpers import _split_csv
from ..models import (
    AuthSession,
    PasskeyChallenge,
    PasskeyCredential,
    User,
    UserInvite,
)
from ..schemas import (
    AuthSessionResponse,
    DevLoginRequest,
    PasskeyAuthenticationOptionsRequest,
    PasskeyAuthenticationOptionsResponse,
    PasskeyAuthenticationVerifyRequest,
    PasskeyRegistrationOptionsRequest,
    PasskeyRegistrationOptionsResponse,
    PasskeyRegistrationVerifyRequest,
    UserAdminRead,
    UserCreateRequest,
    UserInviteResponse,
    UserRead,
    UserResetResponse,
)
from ..services.ynab import _delete_keychain_api_key

log = logging.getLogger("finance_app.api")

router = APIRouter()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _resolve_user_verification(raw_value: str | None) -> UserVerificationRequirement | None:
    if not raw_value:
        return None
    normalized = raw_value.strip().lower()
    mapping = {
        "required": UserVerificationRequirement.REQUIRED,
        "preferred": UserVerificationRequirement.PREFERRED,
        "discouraged": UserVerificationRequirement.DISCOURAGED,
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported PASSKEY_USER_VERIFICATION value: {raw_value}")
    return mapping[normalized]


with contextlib.suppress(ValueError):
    # Feature already configured elsewhere; keep the existing setting.
    fido2_features.webauthn_json_mapping.enabled = True

allowed_origins = _split_csv(settings.allowed_origins) or ["http://localhost:5173"]
passkey_origins = _split_csv(settings.passkey_origins) or allowed_origins
passkey_rp = PublicKeyCredentialRpEntity(
    name=settings.passkey_rp_name,
    id=settings.passkey_rp_id,
)


def _verify_passkey_origin(origin: str) -> bool:
    return origin in passkey_origins


passkey_server = Fido2Server(passkey_rp, verify_origin=_verify_passkey_origin)
passkey_user_verification = _resolve_user_verification(settings.passkey_user_verification)
passkey_challenge_ttl = dt.timedelta(seconds=settings.passkey_challenge_ttl_seconds)
passkey_invite_ttl = dt.timedelta(seconds=settings.passkey_invite_ttl_seconds)
auth_session_ttl = dt.timedelta(seconds=settings.auth_session_ttl_seconds)


def _build_user_entity(user: User) -> PublicKeyCredentialUserEntity:
    name = user.username or user.display_name or "user"
    display_name = user.display_name or name
    user_id = uuid.UUID(user.id).bytes
    return PublicKeyCredentialUserEntity(id=user_id, name=name, display_name=display_name)


def _load_passkey_records(db: Session, user_id: str | None) -> list[PasskeyCredential]:
    stmt = select(PasskeyCredential)
    if user_id:
        stmt = stmt.where(PasskeyCredential.user_id == user_id)
    return list(db.scalars(stmt))


def _to_attested_credentials(passkeys: list[PasskeyCredential]) -> list[AttestedCredentialData]:
    credentials: list[AttestedCredentialData] = []
    for passkey in passkeys:
        try:
            credentials.append(AttestedCredentialData(passkey.credential_data))
        except Exception:
            log.warning("Skipping invalid passkey credential data for %s", passkey.id)
    return credentials


def _prune_expired_challenges(db: Session) -> None:
    now = dt.datetime.now(dt.UTC)
    db.execute(delete(PasskeyChallenge).where(PasskeyChallenge.expires_at < now))
    db.commit()


def _get_challenge(db: Session, challenge_id: str, expected_type: str) -> PasskeyChallenge:
    challenge = db.get(PasskeyChallenge, challenge_id)
    if not challenge or challenge.challenge_type != expected_type:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Challenge not found.")
    if challenge.expires_at < dt.datetime.now(dt.UTC):
        db.delete(challenge)
        db.commit()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Challenge expired.")
    return challenge


def _extract_transports(credential: dict) -> list[str] | None:
    response = credential.get("response") if isinstance(credential, dict) else None
    if isinstance(response, dict):
        transports = response.get("transports")
        if isinstance(transports, list) and all(isinstance(item, str) for item in transports):
            return transports
    return None


def _bootstrap_admin_if_empty(
    db: Session,
    username: str | None,
    display_name: str | None,
) -> User | None:
    existing = db.scalar(select(User.id))
    if existing:
        return None
    if not username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username is required for the initial admin account.",
        )
    if not display_name or not display_name.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Display name is required for the initial admin account.",
        )
    user = User(
        username=username,
        display_name=display_name.strip(),
        is_admin=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    _assign_existing_data_to_user(db, user.id)
    return user


def _get_user_by_username(db: Session, username: str) -> User | None:
    return db.scalar(select(User).where(User.username == username))


def _get_invite(db: Session, token: str) -> UserInvite:
    invite = db.scalar(select(UserInvite).where(UserInvite.token == token))
    if not invite:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invite not found.")
    if invite.used_at is not None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invite already used.")
    if invite.expires_at < dt.datetime.now(dt.UTC):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invite expired.")
    return invite


def _create_invite(db: Session, user: User) -> UserInvite:
    now = dt.datetime.now(dt.UTC)
    invite = UserInvite(
        id=str(uuid.uuid4()),
        user_id=user.id,
        token=_b64url_encode(uuid.uuid4().bytes),
        created_at=now,
        expires_at=now + passkey_invite_ttl,
    )
    db.add(invite)
    db.commit()
    db.refresh(invite)
    return invite


def _create_session(db: Session, user_id: str) -> AuthSession:
    now = dt.datetime.now(dt.UTC)
    session = AuthSession(
        id=str(uuid.uuid4()),
        user_id=user_id,
        created_at=now,
        expires_at=now + auth_session_ttl,
    )
    db.add(session)
    db.commit()
    db.refresh(session)
    return session


@router.post(
    "/auth/passkeys/registration/options",
    response_model=PasskeyRegistrationOptionsResponse,
)
def passkey_registration_options(
    payload: PasskeyRegistrationOptionsRequest,
    request: Request,
    db: Session = Depends(get_db_session),
):
    _prune_expired_challenges(db)
    invite: UserInvite | None = None
    user = _bootstrap_admin_if_empty(db, payload.username, payload.display_name)
    if user is None:
        if payload.invite_token:
            invite = _get_invite(db, payload.invite_token)
            user = invite.user
        else:
            token = _get_bearer_token(request)
            if token:
                user = _require_session(request, db)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invite token or active session required.",
        )

    existing = _load_passkey_records(db, user.id)
    credentials = _to_attested_credentials(existing)

    options, state = passkey_server.register_begin(
        _build_user_entity(user),
        credentials=credentials or None,
        resident_key_requirement=ResidentKeyRequirement.REQUIRED,
        user_verification=passkey_user_verification,
    )

    raw_uv = state.get("user_verification")
    uv_value = raw_uv.value if hasattr(raw_uv, "value") else None
    now = dt.datetime.now(dt.UTC)
    challenge = PasskeyChallenge(
        id=str(uuid.uuid4()),
        user_id=user.id,
        invite_id=invite.id if invite else None,
        challenge=state["challenge"],
        user_verification=uv_value,
        challenge_type="registration",
        created_at=now,
        expires_at=now + passkey_challenge_ttl,
    )
    db.add(challenge)
    db.commit()

    options_dict = dict(options)
    public_key = options_dict.get("publicKey")
    return PasskeyRegistrationOptionsResponse(
        challenge_id=challenge.id,
        options=public_key if isinstance(public_key, dict) else options_dict,
    )


@router.post(
    "/auth/passkeys/registration/verify",
    status_code=status.HTTP_201_CREATED,
)
def passkey_registration_verify(
    payload: PasskeyRegistrationVerifyRequest,
    db: Session = Depends(get_db_session),
):
    challenge = _get_challenge(db, payload.challenge_id, "registration")
    if not challenge.user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing user context.")

    state = {
        "challenge": challenge.challenge,
        "user_verification": _resolve_user_verification(challenge.user_verification),
    }

    try:
        auth_data = passkey_server.register_complete(state, payload.credential)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    credential_data = auth_data.credential_data
    if not credential_data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Missing credential data."
        )

    credential_id = _b64url_encode(credential_data.credential_id)
    existing = db.scalar(
        select(PasskeyCredential).where(PasskeyCredential.credential_id == credential_id)
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Passkey already registered.",
        )

    transports = _extract_transports(payload.credential)
    passkey = PasskeyCredential(
        user_id=challenge.user_id,
        credential_id=credential_id,
        credential_data=bytes(credential_data),
        sign_count=auth_data.counter,
        transports=json.dumps(transports) if transports else None,
        aaguid=credential_data.aaguid.hex() if credential_data.aaguid else None,
        created_at=dt.datetime.now(dt.UTC),
    )
    db.add(passkey)
    if challenge.invite_id:
        invite = db.get(UserInvite, challenge.invite_id)
        if invite and invite.used_at is None:
            invite.used_at = dt.datetime.now(dt.UTC)
    db.delete(challenge)
    db.commit()

    return {"status": "ok", "credential_id": credential_id}


@router.post(
    "/auth/passkeys/authentication/options",
    response_model=PasskeyAuthenticationOptionsResponse,
)
def passkey_authentication_options(
    payload: PasskeyAuthenticationOptionsRequest,
    db: Session = Depends(get_db_session),
):
    _prune_expired_challenges(db)
    user_id: str | None = None
    if payload.username:
        user = db.scalar(select(User).where(User.username == payload.username))
        if not user:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")
        user_id = user.id

    passkeys = _load_passkey_records(db, user_id)
    if not passkeys:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No passkeys registered.")

    credentials = _to_attested_credentials(passkeys)
    options, state = passkey_server.authenticate_begin(
        credentials,
        user_verification=passkey_user_verification,
    )

    raw_uv = state.get("user_verification")
    uv_value = raw_uv.value if hasattr(raw_uv, "value") else None
    now = dt.datetime.now(dt.UTC)
    challenge = PasskeyChallenge(
        id=str(uuid.uuid4()),
        user_id=user_id,
        challenge=state["challenge"],
        user_verification=uv_value,
        challenge_type="authentication",
        created_at=now,
        expires_at=now + passkey_challenge_ttl,
    )
    db.add(challenge)
    db.commit()

    options_dict = dict(options)
    public_key = options_dict.get("publicKey")
    return PasskeyAuthenticationOptionsResponse(
        challenge_id=challenge.id,
        options=public_key if isinstance(public_key, dict) else options_dict,
    )


@router.post(
    "/auth/passkeys/authentication/verify",
    response_model=AuthSessionResponse,
)
def passkey_authentication_verify(
    payload: PasskeyAuthenticationVerifyRequest,
    db: Session = Depends(get_db_session),
):
    challenge = _get_challenge(db, payload.challenge_id, "authentication")
    passkeys = _load_passkey_records(db, challenge.user_id)
    if not passkeys:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No passkeys registered.")

    state = {
        "challenge": challenge.challenge,
        "user_verification": _resolve_user_verification(challenge.user_verification),
    }
    credentials = _to_attested_credentials(passkeys)

    try:
        passkey_server.authenticate_complete(state, credentials, payload.credential)
        auth_response = AuthenticationResponse.from_dict(payload.credential)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    credential_id_raw = auth_response.id
    if isinstance(credential_id_raw, str):
        credential_id_raw = _b64url_decode(credential_id_raw)
    credential_id = _b64url_encode(credential_id_raw)
    passkey = db.scalar(
        select(PasskeyCredential).where(PasskeyCredential.credential_id == credential_id)
    )
    if not passkey:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unknown credential.")

    counter = auth_response.response.authenticator_data.counter
    if counter and counter <= passkey.sign_count:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Potentially cloned authenticator detected.",
        )
    if counter:
        passkey.sign_count = counter
    passkey.last_used_at = dt.datetime.now(dt.UTC)

    db.delete(challenge)
    session = _create_session(db, passkey.user_id)
    user = db.get(User, passkey.user_id)

    return AuthSessionResponse(
        token=session.id,
        user=UserRead.model_validate(user),
    )


@router.post("/auth/dev-login", response_model=AuthSessionResponse)
def dev_login(payload: DevLoginRequest, db: Session = Depends(get_db_session)):
    """Password-free login for local development only.

    Gated behind ``FINANCE_DEV_LOGIN``. When the flag is off the endpoint
    behaves as if it does not exist (404) so it cannot be probed in production.
    Logs in (creating on first use) a dev admin user and issues a normal
    session, bypassing the passkey flow entirely.
    """
    if not settings.dev_login_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found.")

    username = (payload.username or settings.dev_login_username).strip()
    if not username:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is required.")

    user = _get_user_by_username(db, username)
    if user is None:
        # First user in an empty DB inherits any orphaned data, mirroring the
        # passkey bootstrap path.
        is_first = db.scalar(select(User.id)) is None
        user = User(username=username, display_name=username, is_admin=True)
        db.add(user)
        db.commit()
        db.refresh(user)
        if is_first:
            _assign_existing_data_to_user(db, user.id)

    session = _create_session(db, user.id)
    return AuthSessionResponse(token=session.id, user=UserRead.model_validate(user))


@router.get("/auth/setup-status")
def get_setup_status(db: Session = Depends(get_db_session)):
    """Public hint so the login screen can steer first-time users.

    When the database is empty there is nothing to authenticate against, so the
    UI should point people at "create the first admin" instead of letting the
    login button silently fail.
    """
    has_users = db.scalar(select(User.id)) is not None
    has_passkeys = db.scalar(select(PasskeyCredential.id)) is not None
    return {
        "needs_setup": not has_users,
        "has_passkeys": has_passkeys,
        "dev_login": settings.dev_login_enabled,
    }


@router.get("/auth/session", response_model=UserRead)
def get_auth_session(request: Request, db: Session = Depends(get_db_session)):
    user = _require_session(request, db)
    return UserRead.model_validate(user)


@router.post("/auth/logout")
def logout(request: Request, db: Session = Depends(get_db_session)):
    token = _get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing session token."
        )

    session = db.get(AuthSession, token)
    if session:
        db.delete(session)
        db.commit()
    return {"status": "ok"}


@router.get("/auth/users", response_model=list[UserAdminRead])
def list_users(request: Request, db: Session = Depends(get_db_session)):
    current_user = _require_session(request, db)
    _require_admin(current_user)

    users = db.scalars(select(User).order_by(User.created_at.asc())).all()
    passkey_counts = dict(
        db.execute(
            select(PasskeyCredential.user_id, func.count(PasskeyCredential.id)).group_by(
                PasskeyCredential.user_id
            )
        )
        .tuples()
        .all()
    )

    return [
        UserAdminRead(
            id=user.id,
            username=user.username,
            display_name=user.display_name,
            is_admin=user.is_admin,
            created_at=user.created_at,
            passkey_count=int(passkey_counts.get(user.id, 0)),
        )
        for user in users
    ]


@router.post("/auth/users", response_model=UserInviteResponse, status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreateRequest,
    request: Request,
    db: Session = Depends(get_db_session),
):
    current_user = _require_session(request, db)
    _require_admin(current_user)

    existing = _get_user_by_username(db, payload.username)
    if existing:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="User already exists.")

    user = User(username=payload.username, display_name=payload.display_name.strip())
    db.add(user)
    db.commit()
    db.refresh(user)

    invite = _create_invite(db, user)

    return UserInviteResponse(
        user=UserRead.model_validate(user),
        invite_token=invite.token,
        invite_expires_at=invite.expires_at,
    )


@router.post("/auth/users/{user_id}/reset", response_model=UserResetResponse)
def reset_user_access(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db_session),
):
    current_user = _require_session(request, db)
    _require_admin(current_user)

    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    passkeys_revoked = int(
        db.scalar(
            select(func.count())
            .select_from(PasskeyCredential)
            .where(PasskeyCredential.user_id == user_id)
        )
        or 0
    )
    sessions_revoked = int(
        db.scalar(
            select(func.count()).select_from(AuthSession).where(AuthSession.user_id == user_id)
        )
        or 0
    )

    db.execute(delete(PasskeyCredential).where(PasskeyCredential.user_id == user_id))
    db.execute(delete(AuthSession).where(AuthSession.user_id == user_id))
    db.execute(delete(PasskeyChallenge).where(PasskeyChallenge.user_id == user_id))
    db.execute(delete(UserInvite).where(UserInvite.user_id == user_id))

    invite = _create_invite(db, user)

    return UserResetResponse(
        user=UserRead.model_validate(user),
        invite_token=invite.token,
        invite_expires_at=invite.expires_at,
        passkeys_revoked=passkeys_revoked,
        sessions_revoked=sessions_revoked,
    )


@router.delete("/auth/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db_session),
):
    current_user = _require_session(request, db)
    _require_admin(current_user)

    if current_user.id == user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete your own account.",
        )

    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found.")

    if user.is_admin:
        admin_count = int(
            db.scalar(select(func.count()).select_from(User).where(User.is_admin.is_(True))) or 0
        )
        if admin_count <= 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete the last admin user.",
            )

    _delete_keychain_api_key(user_id)
    db.delete(user)
    db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)
