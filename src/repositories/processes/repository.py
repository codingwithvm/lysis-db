from src.infra.db import run_query


def fetch_process_count():
    sql = """
        SELECT COUNT(*) AS total_processos
        FROM PRO_PROCESSO_VALENCA
    """
    return run_query(sql)

def fetch_by_origin():
    sql = """
        SELECT COUNT(*) AS total,
               T02.DES_ATRIBUTO AS origin
        FROM PRO_PROCESSO_VALENCA T01
        INNER JOIN DAR_DOMINIO_ATRIBUTO_VALENCA T02
            ON T01.TIP_ORIGEM_PROCESSO = T02.VAL_ATRIBUTO
           AND T02.NOM_ATRIBUTO = 'TIP_ORIGEM_PROCESSO'
        GROUP BY T02.DES_ATRIBUTO
        ORDER BY total DESC
    """
    return run_query(sql)

def fetch_by_status():
    sql = """
        SELECT COUNT(*) AS total,
               T02.DES_ATRIBUTO AS status
        FROM PRO_PROCESSO_VALENCA T01
        INNER JOIN DAR_DOMINIO_ATRIBUTO_VALENCA T02
            ON T01.STA_PROCESSO = T02.VAL_ATRIBUTO
           AND T02.NOM_ATRIBUTO = 'STA_PROCESSO'
        GROUP BY T02.DES_ATRIBUTO
        ORDER BY T02.DES_ATRIBUTO
    """
    return run_query(sql)

def fetch_by_matter():
    sql = """
        SELECT COUNT(*) AS total,
               COALESCE(T02.NOM_MATERIA, 'Não informado') AS subject
        FROM PRO_PROCESSO_VALENCA T01
        LEFT JOIN MAT_MATERIA_VALENCA T02
            ON T01.ISN_MATERIA = T02.ISN_MATERIA
        GROUP BY T02.NOM_MATERIA
        ORDER BY T02.NOM_MATERIA
    """
    return run_query(sql)

def fetch_by_group():
    sql = """
        SELECT COUNT(*) AS total,
               COALESCE(T02.DSC_GRUPO_PROCESSO, 'Não informado') AS process_group
        FROM PRO_PROCESSO_VALENCA T01
        LEFT JOIN GPP_GRUPO_PROCESSO_VALENCA T02
            ON T01.ISN_GRUPO_PROCESSO = T02.ISN_GRUPO_PROCESSO
        GROUP BY T02.DSC_GRUPO_PROCESSO
        ORDER BY T02.DSC_GRUPO_PROCESSO
    """
    return run_query(sql)

def fetch_by_organization():
    sql = """
        SELECT COUNT(*) AS total,
               COALESCE(T03.DSC_ORGAO, 'Não informado') AS agency
        FROM PRO_PROCESSO_VALENCA T01
        LEFT JOIN INS_INSTANCIA_VALENCA T02
            ON T01.ISN_PROCESSO = T02.ISN_PROCESSO
        LEFT JOIN ORG_ORGAO_VALENCA T03
            ON T02.ISN_ORGAO = T03.ISN_ORGAO
        GROUP BY T03.DSC_ORGAO
        ORDER BY T03.DSC_ORGAO
    """
    return run_query(sql)
