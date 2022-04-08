import base64
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from starlette import status

from mars.atomicity import AtomicPhaseGroup
from mars.models.file import CreateFile, File, FileDAO
from mars.models.idempotency_key import IdempotencyKeyRecoveryPoint

router = APIRouter(prefix="/files")


class FileCreateRecoveryPoint:
    FILE_CREATED = "FILE_CREATED"
    FILE_UPLOADED_TO_S3 = "FILE_UPLOADED_TO_S3"


@router.get("")
async def get_files():
    pass


@router.post("")
async def create_file(
    create_file_input: CreateFile,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    key = await atomic_group.get_idempotency_key()

    while True:
        match key.recovery_point:
            case IdempotencyKeyRecoveryPoint.STARTED:
                # when starting, the file should be created in the db
                async with atomic_group.atomic_phase() as phase:
                    await FileDAO.create(
                        phase.session,
                        idempotency_key_id=key.id,
                        user_id=phase.user_id,
                        name=create_file_input.name,
                        s3_key=create_file_input.get_s3_key(),
                    )
                    phase.set_recovery_point(FileCreateRecoveryPoint.FILE_CREATED)
            case FileCreateRecoveryPoint.FILE_CREATED:
                # When the file has been created, it should be uploaded to S3
                async with atomic_group.atomic_phase() as phase:
                    file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
                    if file_dao is None:
                        raise RuntimeError(f"Bug! Should have file for key at {FileCreateRecoveryPoint.FILE_CREATED}")

                    # TODO: Upload the file to S3
                    # aioboto3.s3.upload_file(...)
                    _ = base64.b64decode(create_file_input.content_base64)

                    phase.set_recovery_point(FileCreateRecoveryPoint.FILE_UPLOADED_TO_S3)
            case FileCreateRecoveryPoint.FILE_UPLOADED_TO_S3:
                # When the file has been uploaded to S3, it should be finalized
                async with atomic_group.atomic_phase() as phase:
                    file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
                    file_dao.update(upload_completed_at=datetime.now(timezone.utc))
                    phase.set_response(status.HTTP_200_OK, File.from_orm(file_dao))
            case IdempotencyKeyRecoveryPoint.FINISHED:
                break
            case _:
                raise Exception(f"Unknown recovery point: {key.recovery_point}")

    return atomic_group.get_response()


@router.get("/{file_id}")
async def get_file(file_id: int):
    pass


@router.post("/{file_id}/versions")
async def create_file_version(
    file_id: int,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    return atomic_group.get_response()


@router.delete("/{file_id}")
async def delete_file(
    file_id: int,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    return atomic_group.get_response()
