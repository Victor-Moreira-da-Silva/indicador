"""Queries de referência para coleta por módulo.

As consultas abaixo foram organizadas por área para facilitar agendamento,
execução incremental e manutenção.
"""

QUERIES: dict[str, dict[str, str]] = {
    "Diretoria": {
        "internacao_e_altas": """
SELECT 'Internados Hoje' indic, Count(*) Quant
FROM dbamv.atendime
WHERE dt_atendimento between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1) and cd_atendimento_pai is null
AND tp_atendimento = 'I'
UNION ALL
SELECT 'Altas Hospitalar' indic, Count(*) Quant
FROM dbamv.atendime
WHERE dt_alta between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1) and cd_atendimento_pai is null
AND tp_atendimento = 'I'
""",
        "obitos_mes": """
SELECT TO_CHAR(A.DT_ALTA, 'YYYY-MM') AS mes_ano, COUNT(*) AS qtd_obitos
FROM DBAMV.ATENDIME A
WHERE A.SN_OBITO = 'S'
  AND A.DT_ALTA IS NOT NULL
  AND A.DT_ALTA >= ADD_MONTHS(TRUNC(SYSDATE), -12)
GROUP BY TO_CHAR(A.DT_ALTA, 'YYYY-MM')
ORDER BY TO_CHAR(A.DT_ALTA, 'YYYY-MM')
""",
    },
    "Enfermagem": {
        "fila_recepcao": """
SELECT COUNT(DISTINCT TA.CD_TRIAGEM_ATENDIMENTO) AS QTDE_PACIENTES_FILA
FROM TRIAGEM_ATENDIMENTO TA
JOIN SACR_TEMPO_PROCESSO STP ON STP.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
WHERE TA.DH_PRE_ATENDIMENTO >= SYSDATE - 1
  AND STP.CD_TIPO_TEMPO_PROCESSO = 12
""",
        "fila_triagem": """
SELECT NVL(SUM(Quant),0) quantidade
FROM (
  SELECT sacr_tempo_processo.cd_triagem_atendimento, Count(*) quant
  FROM dbamv.sacr_tempo_processo, dbamv.triagem_atendimento
  WHERE sacr_tempo_processo.cd_triagem_atendimento = triagem_atendimento.cd_triagem_atendimento
    AND trunc(triagem_atendimento.dh_pre_atendimento) = trunc(SYSDATE)
    AND triagem_atendimento.cd_multi_empresa IN(2)
  GROUP BY sacr_tempo_processo.cd_triagem_atendimento
  HAVING Count(*) = 1
)
""",
    },
    "UTI": {
        "internados": """
SELECT Count(*) Quant
FROM dbamv.atendime
WHERE dt_atendimento between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1)
and cd_atendimento_pai is null
AND tp_atendimento = 'I'
"""
    },
    "Centro Cirúrgico": {
        "cirurgias_status_mes": """
SELECT TO_CHAR(DT_AVISO_CIRURGIA, 'YYYY-MM') AS MES, TP_SITUACAO, COUNT(*) AS QTDE
FROM DBAMV.AVISO_CIRURGIA
WHERE DT_AVISO_CIRURGIA IS NOT NULL
GROUP BY TO_CHAR(DT_AVISO_CIRURGIA, 'YYYY-MM'), TP_SITUACAO
ORDER BY MES, TP_SITUACAO
"""
    },
    "Farmácia": {
        "medicamentos_alto_risco": """
SELECT TO_CHAR(PM.DT_PRE_MED,'YYYY-MM') MES, PRD.DS_PRODUTO, COUNT(*) QTDE
FROM DBAMV.PRE_MED PM, DBAMV.ITPRE_MED IPM, DBAMV.PRODUTO PRD
WHERE PM.CD_PRE_MED = IPM.CD_PRE_MED
AND PRD.CD_PRODUTO = IPM.CD_PRODUTO
AND NVL(IPM.SN_CANCELADO,'N') = 'N'
AND PM.DT_PRE_MED >= ADD_MONTHS(TRUNC(SYSDATE,'MM'), -11)
GROUP BY TO_CHAR(PM.DT_PRE_MED,'YYYY-MM'), PRD.DS_PRODUTO
ORDER BY MES, QTDE DESC
"""
    },
}
