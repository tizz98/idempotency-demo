import base64
from datetime import datetime, timezone
from hashlib import md5

import aioboto3
from starlette import status

from mars.atomicity import AtomicPhaseGroup
from mars.db import read_committed_session_maker
from mars.models.base import PaginationParams
from mars.models.file import CreateFile, File, FileDAO, UpdateFile
from mars.models.idempotency_key import IdempotencyKeyDAO, IdempotencyKeyRecoveryPoint


class FileRecoveryPoint:
    FILE_CREATED = "FILE_CREATED"
    FILE_UPDATED = "FILE_UPDATED"
    FILE_DELETED_IN_S3 = "FILE_DELETED_IN_S3"
    FILE_UPLOADED_TO_S3 = "FILE_UPLOADED_TO_S3"


class FileService:
    def __init__(self, boto_session: aioboto3.Session):
        self._boto_session = boto_session

    async def put_object(self, key: str, body_bytes: bytes) -> dict:
        async with self._boto_session.client("s3") as s3:
            return await s3.put_object(
                Body=body_bytes,
                ContentMD5=base64.b64encode(md5(body_bytes).digest()).decode("utf-8"),
                Key=key,
                Bucket="mars-files",
            )

    async def delete_object(self, key: str) -> dict:
        async with self._boto_session.client("s3") as s3:
            return await s3.delete_object(Bucket="mars-files", Key=key)

    async def get_file(self, file_id: int) -> File:
        async with read_committed_session_maker() as session, session.begin():
            file_dao = await FileDAO.get_by_id(session, file_id)

        return File.from_orm(file_dao)

    async def list_files(self, pagination_params: PaginationParams) -> list[File]:
        async with read_committed_session_maker() as session, session.begin():
            file_dao_list = await FileDAO.get_all(session, pagination_params)

        return [File.from_orm(file_dao) for file_dao in file_dao_list]


class HandleFileAction:
    def __init__(self, file_service: FileService, atomic_group: AtomicPhaseGroup):
        self._file_service = file_service
        self._atomic_group = atomic_group

    async def execute(self) -> None:
        while True:
            key = await self._atomic_group.get_idempotency_key()
            match (key.recovery_point, key.request_method):
                case IdempotencyKeyRecoveryPoint.STARTED, "POST":
                    await self._create_file(key)
                case IdempotencyKeyRecoveryPoint.STARTED, "PATCH":
                    await self._update_file(key)
                case IdempotencyKeyRecoveryPoint.STARTED, "DELETE":
                    await self._delete_file_s3(key)
                case FileRecoveryPoint.FILE_CREATED, _:
                    await self._upload_file(key)
                case FileRecoveryPoint.FILE_UPDATED, _:
                    await self._upload_file(key)
                case FileRecoveryPoint.FILE_UPLOADED_TO_S3, _:
                    await self._complete_file_upload(key)
                case FileRecoveryPoint.FILE_DELETED_IN_S3, _:
                    await self._delete_file_db(key)
                case IdempotencyKeyRecoveryPoint.FINISHED, _:
                    break
                case _:
                    raise Exception(f"Unknown recovery point: {key.recovery_point}")

    async def _create_file(self, key: IdempotencyKeyDAO):
        # when starting, the file should be created in the db
        create_file_input = CreateFile.construct(**key.request_params)

        async with self._atomic_group.atomic_phase() as phase:
            await FileDAO.create(
                phase.session,
                idempotency_key_id=key.id,
                user_id=phase.user_id,
                name=create_file_input.name,
                s3_key=create_file_input.get_s3_key(),
            )
            phase.set_recovery_point(FileRecoveryPoint.FILE_CREATED)

    async def _update_file(self, key: IdempotencyKeyDAO):
        file_id = int(key.request_path_params["file_id"])
        update_file_input = UpdateFile.construct(**key.request_params)

        async with self._atomic_group.atomic_phase() as phase:
            file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
            if file_dao is None:
                raise RuntimeError("Bug! Should have file for key")
            if file_dao.id != file_id:
                raise RuntimeError("Bug! File id mismatch")

            update_data = update_file_input.dict(exclude_unset=True)
            update_data.pop("content_base64", None)
            file_dao.update(**update_data)

            phase.set_recovery_point(FileRecoveryPoint.FILE_UPDATED)

    async def _upload_file(self, key: IdempotencyKeyDAO):
        # When the file has been created, it should be uploaded to S3
        async with self._atomic_group.atomic_phase() as phase:
            file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
            if file_dao is None:
                raise RuntimeError(f"Bug! Should have file for key at {FileRecoveryPoint.FILE_CREATED}")

            body_bytes = base64.b64decode(key.request_params["content_base64"])
            await self._file_service.put_object(file_dao.s3_key, body_bytes)

            phase.set_recovery_point(FileRecoveryPoint.FILE_UPLOADED_TO_S3)

    async def _complete_file_upload(self, key: IdempotencyKeyDAO):
        # When the file has been uploaded to S3, it should be finalized
        async with self._atomic_group.atomic_phase() as phase:
            file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
            if file_dao is None:
                raise RuntimeError(f"Bug! Should have file for key at {FileRecoveryPoint.FILE_UPLOADED_TO_S3}")

            file_dao.update(upload_completed_at=datetime.now(timezone.utc))
            phase.set_response(status.HTTP_200_OK, File.from_orm(file_dao))

    async def _delete_file_s3(self, key: IdempotencyKeyDAO):
        async with self._atomic_group.atomic_phase() as phase:
            file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
            if file_dao is None:
                raise RuntimeError("Bug! Should have file for key")
            if file_dao.id != int(key.request_path_params["file_id"]):
                raise RuntimeError("Bug! File id mismatch")

            await self._file_service.delete_object(file_dao.s3_key)
            phase.set_recovery_point(FileRecoveryPoint.FILE_DELETED_IN_S3)

    async def _delete_file_db(self, key: IdempotencyKeyDAO):
        async with self._atomic_group.atomic_phase() as phase:
            file_dao = await FileDAO.get_by_idempotency_key(phase.session, key.id)
            if file_dao is None:
                raise RuntimeError("Bug! Should have file for key")
            if file_dao.id != int(key.request_path_params["file_id"]):
                raise RuntimeError("Bug! File id mismatch")

            await file_dao.delete(phase.session)
