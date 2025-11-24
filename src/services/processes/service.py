from ...repositories.processes.repository import (
    fetch_by_group,
    fetch_by_matter,
    fetch_by_organization,
    fetch_by_origin,
    fetch_by_status,
    fetch_process_count,
)


def get_process_count():
    result = fetch_process_count()
    return {"total": result[0]["total_processos"]}

def get_origin_stats():
    return fetch_by_origin()

def get_status_stats():
    return fetch_by_status()

def get_matter_stats():
    return fetch_by_matter()

def get_group_stats():
    return fetch_by_group()

def get_organization_stats():
    return fetch_by_organization()
