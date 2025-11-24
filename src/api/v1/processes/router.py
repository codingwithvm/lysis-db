from fastapi import APIRouter

from ....services.processes.service import (
    get_group_stats,
    get_matter_stats,
    get_organization_stats,
    get_origin_stats,
    get_process_count,
    get_status_stats,
)

router = APIRouter(prefix="/api/v1/processes", tags=["Processes"])

@router.get("/count")
def process_count():
    return get_process_count()

@router.get("/by-origin")
def processes_by_origin():
    return get_origin_stats()

@router.get("/by-status")
def processes_by_status():
    return get_status_stats()

@router.get("/by-matter")
def processes_by_matter():
    return get_matter_stats()

@router.get("/by-group")
def processes_by_group():
    return get_group_stats()

@router.get("/by-organization")
def processes_by_organization():
    return get_organization_stats()
