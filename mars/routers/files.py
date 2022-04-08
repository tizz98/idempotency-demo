import aioboto3
from fastapi import APIRouter, Depends

from mars.atomicity import AtomicPhaseGroup
from mars.models.base import PaginationParams
from mars.models.file import File
from mars.services.file import FileService, HandleFileAction

router = APIRouter(prefix="/files")


@router.get("", response_model=list[File])
async def get_files(pagination_params: PaginationParams):
    return FileService(aioboto3.Session()).list_files(pagination_params)


@router.post("")
async def create_file(atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency)):
    await HandleFileAction(FileService(aioboto3.Session()), atomic_group).execute()
    return atomic_group.get_response()


@router.get("/{file_id}", response_model=File)
async def get_file(file_id: int):
    return FileService(aioboto3.Session()).get_file(file_id)


@router.patch("/{file_id}")
async def update_file(
    file_id: int,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    await HandleFileAction(FileService(aioboto3.Session()), atomic_group).execute()
    return atomic_group.get_response()


@router.post("/{file_id}/versions")
async def create_file_version(
    file_id: int,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    await HandleFileAction(FileService(aioboto3.Session()), atomic_group).execute()
    return atomic_group.get_response()


@router.delete("/{file_id}")
async def delete_file(
    file_id: int,
    atomic_group: AtomicPhaseGroup = Depends(AtomicPhaseGroup.dependency),
):
    await HandleFileAction(FileService(aioboto3.Session()), atomic_group).execute()
    return atomic_group.get_response()
