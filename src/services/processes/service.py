from datetime import datetime
from typing import Optional, List, Dict

from ...repositories.processes.repository import (
    fetch_by_group,
    fetch_by_matter,
    fetch_by_organization,
    fetch_by_origin,
    fetch_by_origin_capture_last_six_months,
    fetch_by_origin_distribution_last_six_months,
    fetch_by_origin_import_last_six_months,
    fetch_by_origin_registration_by_year_range,
    fetch_by_origin_registration_last_six_months,
    fetch_by_origin_with_date_range,
    fetch_by_origin_with_date_range_detailed,
    fetch_by_origin_with_instance_date_filter,
    fetch_by_status,
    fetch_process_count,
)
from ...schemas.schemas import (
    DateRangeFilter,
    OriginDateFilter,
    YearFilter,
    YearRangeFilter,
)


def _filter_by_date_range(
    data: List[Dict], 
    start_date: Optional[str] = None, 
    end_date: Optional[str] = None
) -> List[Dict]:
    if not start_date and not end_date:
        return data
    
    start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
    end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None
    
    filtered = []
    for item in data:
        if 'data_instancia' not in item or item['data_instancia'] is None:
            continue
        
        item_date = item['data_instancia']
        if isinstance(item_date, str):
            try:
                item_date = datetime.strptime(item_date, '%Y-%m-%d').date()
            except:
                continue
        elif hasattr(item_date, 'date'):
            item_date = item_date.date()
        
        if start and item_date < start:
            continue
        if end and item_date > end:
            continue
        
        filtered.append(item)
    
    return filtered

def _aggregate_results(data: List[Dict], group_key: str) -> List[Dict]:
    grouped = {}
    for item in data:
        key = item.get(group_key, 'Não informado')
        if key not in grouped:
            grouped[key] = {'total': 0, group_key: key}
        grouped[key]['total'] += item.get('total', 0)
    return list(grouped.values())

def get_process_count():
    result = fetch_process_count()
    return {"total": result[0]["total_processos"]}

def get_origin_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = fetch_by_origin()
    
    if start_date or end_date:
        data = _filter_by_date_range(data, start_date, end_date)
    
    return _aggregate_results(data, 'origin')

def get_status_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = fetch_by_status()
    
    if start_date or end_date:
        data = _filter_by_date_range(data, start_date, end_date)
    
    return _aggregate_results(data, 'status')

def get_matter_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = fetch_by_matter()
    
    if start_date or end_date:
        data = _filter_by_date_range(data, start_date, end_date)
    
    return _aggregate_results(data, 'subject')

def get_group_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = fetch_by_group()
    
    if start_date or end_date:
        data = _filter_by_date_range(data, start_date, end_date)
    
    return _aggregate_results(data, 'process_group')

def get_organization_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    data = fetch_by_organization()
    
    if start_date or end_date:
        data = _filter_by_date_range(data, start_date, end_date)
    
    return _aggregate_results(data, 'agency')

def get_by_origin_with_instance_date_filter(filters: OriginDateFilter):
    return fetch_by_origin_with_instance_date_filter(filters)

def get_by_origin_registration_by_year_range(filters: YearRangeFilter):
    return fetch_by_origin_registration_by_year_range(filters)

def get_by_origin_registration_last_six_months(filters: YearFilter):
    return fetch_by_origin_registration_last_six_months(filters)

def get_by_origin_capture_last_six_months(filters: YearFilter):
    return fetch_by_origin_capture_last_six_months(filters)

def get_by_origin_distribution_last_six_months(filters: YearFilter):
    return fetch_by_origin_distribution_last_six_months(filters)

def get_by_origin_import_last_six_months(filters: YearFilter):
    return fetch_by_origin_import_last_six_months(filters)

def get_by_origin_with_date_range(filters: DateRangeFilter):
    return fetch_by_origin_with_date_range(filters)

def get_by_origin_with_date_range_detailed(filters: DateRangeFilter):
    return fetch_by_origin_with_date_range_detailed(filters)