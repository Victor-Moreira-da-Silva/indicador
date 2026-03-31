"""Queries de referência para coleta por módulo.

As consultas abaixo foram organizadas por área para facilitar agendamento,
execução incremental e manutenção.
"""

QUERIES: dict[str, dict[str, str]] = {
    "Diretoria": {
        "evolucao_atendimentos_internacao": """
select competencia, tp_Atendimento, count(1) qtd
from (
SELECT To_Char(dt_atendimento, 'mm/yyyy') competencia ,
Decode(tp_atendimento, 'I', 'Internação', 'U', 'Urgência', 'E', 'Externo', 'A',
'Ambulatório' , 'H' , 'Home Care' , 'B' , 'Busca Ativa' ) tp_atendimento
FROM dbamv.atendime
WHERE dt_Atendimento BETWEEN To_Date('01/01/'|| nvl('2025', to_char(SYSDATE ,
'yyyy') ) , 'dd/mm/yyyy' )
AND Last_Day( To_Date( '31/12/' || nvl( '2030' , to_char(sysdate , 'yyyy') ) ,
'dd/mm/yyyy') + .99999 )
and cd_multi_empresa IN (1)
and cd_atendimento_pai is null
AND tp_atendimento = 'I'
)
GROUP BY competencia , tp_atendimento
ORDER BY competencia
""",
        "evolucao_atendimentos_externos": """
select competencia, tp_Atendimento, count(1) qtd
from (
SELECT To_Char(dt_atendimento, 'mm/yyyy') competencia ,
Decode(tp_atendimento, 'I', 'Internação', 'U', 'Urgência', 'E', 'Externo', 'A',
'Ambulatório' , 'H' , 'Home Care' , 'B' , 'Busca Ativa' ) tp_atendimento
FROM dbamv.atendime
WHERE dt_Atendimento BETWEEN To_Date('01/01/'|| nvl( '2025' , to_char(SYSDATE ,
'yyyy') ) , 'dd/mm/yyyy' )
AND Last_Day( To_Date( '31/12/' || nvl( '2030' , to_char(sysdate , 'yyyy') ) ,
'dd/mm/yyyy') + .99999 )
and cd_multi_empresa IN (1)
and cd_atendimento_pai is null
AND tp_atendimento = 'E'
)
GROUP BY competencia , tp_atendimento
ORDER BY competencia
""",
        "evolucao_atendimentos_ambulatorio": """
select competencia, tp_Atendimento, count(1) qtd
from (
SELECT To_Char(dt_atendimento, 'mm/yyyy') competencia ,
Decode(tp_atendimento, 'I', 'Internação', 'U', 'Urgência', 'E', 'Externo', 'A',
'Ambulatório' , 'H' , 'Home Care' , 'B' , 'Busca Ativa' ) tp_atendimento
FROM dbamv.atendime
WHERE dt_Atendimento BETWEEN To_Date('01/01/'|| nvl( '2025' , to_char(SYSDATE ,
'yyyy') ) , 'dd/mm/yyyy' )
AND Last_Day( To_Date( '31/12/' || nvl( '2030' , to_char(sysdate , 'yyyy') ) ,
'dd/mm/yyyy') + .99999 )
and cd_multi_empresa IN (1)
and cd_atendimento_pai is null
AND tp_atendimento = 'A'
)
GROUP BY competencia , tp_atendimento
ORDER BY competencia
""",
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
UNION ALL
SELECT 'Altas Médicas' indic, Count(*) Quant
FROM dbamv.atendime
WHERE dt_alta_medica between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1) and cd_atendimento_pai is null
AND tp_atendimento = 'I'
UNION ALL
SELECT 'Previsão de Altas' indic, Count(*) Quant
FROM dbamv.atendime
WHERE dt_prevista_alta between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1) and cd_atendimento_pai is null
AND tp_atendimento = 'I'
UNION ALL
SELECT 'Previsão de Altas com Atraso' indic, Count(*) Quant
FROM dbamv.atendime
WHERE dt_prevista_alta between trunc(sysdate) and sysdate
and cd_multi_empresa IN (1) and cd_atendimento_pai is null
AND dt_alta IS null
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
        "evolucao_atendimentos_urgencia": """
select competencia, tp_Atendimento, count(1) qtd
from (
SELECT To_Char(dt_atendimento, 'mm/yyyy') competencia ,
Decode(tp_atendimento, 'I', 'Internação', 'U', 'Urgência', 'E', 'Externo', 'A',
'Ambulatório' , 'H' , 'Home Care' , 'B' , 'Busca Ativa' ) tp_atendimento
FROM dbamv.atendime
WHERE dt_Atendimento BETWEEN To_Date('01/01/'|| nvl( '2025' , to_char(SYSDATE ,
'yyyy') ) , 'dd/mm/yyyy' )
AND Last_Day( To_Date( '31/12/' || nvl( '2030' , to_char(sysdate , 'yyyy') ) ,
'dd/mm/yyyy') + .99999 )
and cd_multi_empresa IN (2)
and cd_atendimento_pai is null
AND tp_atendimento = 'U'
)
GROUP BY competencia , tp_atendimento
ORDER BY competencia
""",
        "fila_recepcao": """
SELECT COUNT(DISTINCT TA.CD_TRIAGEM_ATENDIMENTO) AS QTDE_PACIENTES_FILA
FROM TRIAGEM_ATENDIMENTO TA
JOIN SACR_TEMPO_PROCESSO STP ON STP.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
WHERE TA.DH_PRE_ATENDIMENTO >= SYSDATE - 1
  AND STP.CD_TIPO_TEMPO_PROCESSO = 12
  AND NOT EXISTS (
    SELECT 1
    FROM SACR_TEMPO_PROCESSO STP2
    WHERE STP2.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
      AND STP2.CD_TIPO_TEMPO_PROCESSO > 12
  )
""",
        "fila_triagem": """
SELECT NVL(SUM(Quant),0) quantidade
FROM (
  SELECT sacr_tempo_processo.cd_triagem_atendimento, Count(*) quant
  FROM dbamv.sacr_tempo_processo, dbamv.triagem_atendimento
  WHERE sacr_tempo_processo.cd_triagem_atendimento = triagem_atendimento.cd_triagem_atendimento
    AND trunc(triagem_atendimento.dh_pre_atendimento) = trunc(SYSDATE)
    AND triagem_atendimento.cd_multi_empresa IN(2)
    and triagem_atendimento.cd_fila_senha in(7,20,21,23,24,26)
  GROUP BY sacr_tempo_processo.cd_triagem_atendimento
  HAVING Count(*) = 1
)
""",
        "fila_atendimento_medico": """
SELECT COUNT(DISTINCT TA.CD_TRIAGEM_ATENDIMENTO) AS QTDE_PACIENTES_FILA_CONSULTA
FROM TRIAGEM_ATENDIMENTO TA
JOIN SACR_TEMPO_PROCESSO STP ON STP.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
WHERE TA.DH_PRE_ATENDIMENTO >= SYSDATE - 1
  AND STP.CD_TIPO_TEMPO_PROCESSO = 22
  AND NOT EXISTS (
    SELECT 1
    FROM SACR_TEMPO_PROCESSO STP2
    WHERE STP2.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
      AND STP2.CD_TIPO_TEMPO_PROCESSO > 22
  )
""",
        "fila_medicacao": """
SELECT COUNT(DISTINCT TA.CD_TRIAGEM_ATENDIMENTO) AS QTDE_PACIENTES_FILA_MED
FROM TRIAGEM_ATENDIMENTO TA
JOIN SACR_TEMPO_PROCESSO STP ON STP.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
WHERE TA.DH_PRE_ATENDIMENTO >= SYSDATE - 1
  AND STP.CD_TIPO_TEMPO_PROCESSO = 32
  AND TA.CD_TRIAGEM_ATENDIMENTO IN (
    SELECT PS.CD_TRIAGEM_ATENDIMENTO
    FROM SACR_PENDENCIA_SALA PS
    WHERE PS.TP_SITUACAO = 'INI'
      AND PS.TP_PEDIDO = 'MED'
  )
  AND NOT EXISTS (
    SELECT 1
    FROM SACR_TEMPO_PROCESSO STP2
    WHERE STP2.CD_TRIAGEM_ATENDIMENTO = TA.CD_TRIAGEM_ATENDIMENTO
      AND STP2.CD_TIPO_TEMPO_PROCESSO >= 71
  )
""",
        "media_espera_classificacao": """
SELECT nvl(dif_em_hrs,0) dif_em_hrs
FROM
(SELECT CASE WHEN dif_em_hrs < 60 THEN dif_em_hrs
WHEN dif_em_hrs > 60 THEN dif_em_hrs/60 END AS dif_em_hrs FROM
(SELECT round(Avg(dif_em_hrs)) AS dif_em_hrs from (SELECT
senha.cd_triagem_atendimento, round((saida_cad.dh_processo -
senha.dh_processo)*24*60) AS dif_em_hrs
FROM (SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND S.CD_TIPO_TEMPO_PROCESSO in ('1')
AND T.dh_pre_atendimento >= (SYSDATE-1) order by 2,3) senha,
(SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND s.CD_TIPO_TEMPO_PROCESSO in ('11')
AND T.dh_pre_atendimento >= (SYSDATE-1) order by 2,3) saida_cad
WHERE saida_cad.cd_triagem_atendimento = senha.cd_triagem_atendimento)))
""",
        "media_espera_atendimento_medico": """
SELECT nvl(dif_em_hrs,0) dif_em_hrs
FROM
(select ROUND(avg(dif_em_hrs)) as dif_em_hrs from (SELECT
senha.cd_triagem_atendimento, round((saida_cad.dh_processo -
senha.dh_processo)*24*60) AS dif_em_hrs
FROM (SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
SP.CD_TIPO_TEMPO_PROCESSO||' - '
||DECODE(SP.CD_TIPO_TEMPO_PROCESSO,'1','RETIRADA DA SENHA',
'10','CHAMADA PARA CLASSIFICAÇÃO',
'11','ENTRADA NA CLASSIFICAÇÃO',
'12','SAIDA DA CLASSIFICAÇÃO',
'20','CHAMADA PARA CADASTRO',
'21','INÍCIO DO CADASTRO',
'22','FIM DO CADASTRO',
'30','CHAMADA ACOLHIMENTO',
'31','INICIO ACOLHIMENTO',
'33','REAVALIAÇÃO',
'32','FINAL ATENDIMENTO MÉDICO',
'90','ALTA MÉDICA') PROCESSO,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.SACR_TIPO_TEMPO_PROCESSO SP
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE S.CD_TIPO_TEMPO_PROCESSO = SP.CD_TIPO_TEMPO_PROCESSO
AND T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND SP.CD_TIPO_TEMPO_PROCESSO in ('22')
AND T.dh_pre_atendimento BETWEEN (SYSDATE-1) AND SYSDATE order by 2,3) senha,
(SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
SP.CD_TIPO_TEMPO_PROCESSO||' - '
||DECODE(SP.CD_TIPO_TEMPO_PROCESSO,'1','RETIRADA DA SENHA',
'10','CHAMADA PARA CLASSIFICAÇÃO',
'11','ENTRADA NA CLASSIFICAÇÃO',
'12','SAIDA DA CLASSIFICAÇÃO',
'20','CHAMADA PARA CADASTRO',
'21','INÍCIO DO CADASTRO',
'22','FIM DO CADASTRO',
'30','CHAMADA ACOLHIMENTO',
'31','INICIO ACOLHIMENTO',
'33','REAVALIAÇÃO',
'32','FINAL ATENDIMENTO MÉDICO',
'90','ALTA MÉDICA') PROCESSO,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.SACR_TIPO_TEMPO_PROCESSO SP
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE S.CD_TIPO_TEMPO_PROCESSO = SP.CD_TIPO_TEMPO_PROCESSO
AND T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND SP.CD_TIPO_TEMPO_PROCESSO in ('31')
AND T.dh_pre_atendimento BETWEEN (SYSDATE-1) AND SYSDATE order by 2,3)
saida_cad
WHERE saida_cad.cd_triagem_atendimento = senha.cd_triagem_atendimento))
""",
        "media_espera_cadastro": """
SELECT nvl(dif_em_hrs,0) dif_em_hrs
FROM
(SELECT CASE WHEN dif_em_hrs < 60 THEN dif_em_hrs
WHEN dif_em_hrs > 60 THEN dif_em_hrs/60 END AS dif_em_hrs FROM
(select round(avg(dif_em_hrs)) as dif_em_hrs from (SELECT
senha.cd_triagem_atendimento, round((saida_cad.dh_processo -
senha.dh_processo)*24*60) AS dif_em_hrs
FROM (SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
SP.CD_TIPO_TEMPO_PROCESSO||' - '
||DECODE(SP.CD_TIPO_TEMPO_PROCESSO,'1','RETIRADA DA SENHA',
'10','CHAMADA PARA CLASSIFICAÇÃO',
'11','ENTRADA NA CLASSIFICAÇÃO',
'12','SAIDA DA CLASSIFICAÇÃO',
'20','CHAMADA PARA CADASTRO',
'21','INÍCIO DO CADASTRO',
'22','FIM DO CADASTRO',
'30','CHAMADA ACOLHIMENTO',
'31','INICIO ACOLHIMENTO',
'33','REAVALIAÇÃO',
'32','FINAL ATENDIMENTO MÉDICO',
'90','ALTA MÉDICA') PROCESSO,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.SACR_TIPO_TEMPO_PROCESSO SP
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE S.CD_TIPO_TEMPO_PROCESSO = SP.CD_TIPO_TEMPO_PROCESSO
AND T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND SP.CD_TIPO_TEMPO_PROCESSO in ('12')
AND T.dh_pre_atendimento >= (SYSDATE-1) order by 2,3) senha,
(SELECT
S.CD_TRIAGEM_ATENDIMENTO,
T.DS_SENHA,
SP.CD_TIPO_TEMPO_PROCESSO||' - '
||DECODE(SP.CD_TIPO_TEMPO_PROCESSO,'1','RETIRADA DA SENHA',
'10','CHAMADA PARA CLASSIFICAÇÃO',
'11','ENTRADA NA CLASSIFICAÇÃO',
'12','SAIDA DA CLASSIFICAÇÃO',
'20','CHAMADA PARA CADASTRO',
'21','INÍCIO DO CADASTRO',
'22','FIM DO CADASTRO',
'30','CHAMADA ACOLHIMENTO',
'31','INICIO ACOLHIMENTO',
'33','REAVALIAÇÃO',
'32','FINAL ATENDIMENTO MÉDICO',
'90','ALTA MÉDICA') PROCESSO,
s.dh_processo
FROM DBAMV.SACR_TEMPO_PROCESSO S
,DBAMV.SACR_TIPO_TEMPO_PROCESSO SP
,DBAMV.TRIAGEM_ATENDIMENTO T
WHERE S.CD_TIPO_TEMPO_PROCESSO = SP.CD_TIPO_TEMPO_PROCESSO
AND T.CD_TRIAGEM_ATENDIMENTO = S.CD_TRIAGEM_ATENDIMENTO
AND SP.CD_TIPO_TEMPO_PROCESSO in ('21')
AND T.dh_pre_atendimento >= (SYSDATE-1) order by 2,3) saida_cad
WHERE saida_cad.cd_triagem_atendimento = senha.cd_triagem_atendimento)))
""",
        "qtd_pacientes_retorno_7_dias": """
SELECT COUNT(*) AS pacientes_reconsultadores
FROM (
  SELECT A.CD_PACIENTE
  FROM DBAMV.ATENDIME A
  WHERE A.TP_ATENDIMENTO = 'U'
    AND A.CD_MULTI_EMPRESA = 2
    AND A.DT_ATENDIMENTO >= SYSDATE - 7
  GROUP BY A.CD_PACIENTE
  HAVING COUNT(DISTINCT A.CD_ATENDIMENTO) > 2
)
""",
        "retorno": """
WITH sinais_vitais AS (
 SELECT *
 FROM (
 SELECT
 csv.cd_atendimento,
 ic.cd_sinal_vital,
 ic.valor,
 ROW_NUMBER() OVER (
 PARTITION BY csv.cd_atendimento, ic.cd_sinal_vital
 ORDER BY csv.data_coleta DESC
 ) AS rn
 FROM dbamv.coleta_sinal_vital csv
 JOIN dbamv.itcoleta_sinal_vital ic
 ON ic.cd_coleta_sinal_vital = csv.cd_coleta_sinal_vital
 )
 WHERE rn = 1
),
sinais_pivot AS (
 SELECT
 cd_atendimento,
 MAX(CASE WHEN cd_sinal_vital = 1 THEN valor END) AS temperatura,
 MAX(CASE WHEN cd_sinal_vital = 2 THEN valor END) AS frequencia_cardiaca,
 MAX(CASE WHEN cd_sinal_vital = 4 THEN valor END) AS pressao_sistolica,
 MAX(CASE WHEN cd_sinal_vital = 5 THEN valor END) AS pressao_diastolica,
 MAX(CASE WHEN cd_sinal_vital = 11 THEN valor END) AS spo2,
 MAX(CASE WHEN cd_sinal_vital = 13 THEN valor END) AS glicemia
 FROM sinais_vitais
 GROUP BY cd_atendimento
),
medicamentos AS (
 SELECT
 pm.cd_atendimento,
 RTRIM(
 XMLAGG(
 XMLELEMENT(e, prd.ds_produto, ' | ')
 ORDER BY prd.ds_produto
 ).EXTRACT('//text()').GETCLOBVAL(),
 ' | ') AS medicamentos
 FROM dbamv.pre_med pm
 JOIN dbamv.itpre_med ipm
 ON ipm.cd_pre_med = pm.cd_pre_med
 AND NVL(ipm.sn_cancelado, 'N') = 'N'
 JOIN dbamv.produto prd
 ON prd.cd_produto = ipm.cd_produto
 GROUP BY pm.cd_atendimento
)
SELECT
 A.CD_ATENDIMENTO,
 P.CD_PACIENTE,
 P.NM_PACIENTE,
 TO_CHAR(A.HR_ATENDIMENTO, 'DD/MM/YYYY HH24:MI') AS dt_atendimento,
 PR.NM_PRESTADOR AS medico,
 C.CD_CID || ' - ' || C.DS_CID AS cid,
 CASE
 WHEN T.CD_COR_REFERENCIA = 1 THEN 'BRANCO'
 WHEN T.CD_COR_REFERENCIA = 2 THEN 'VERMELHO'
 WHEN T.CD_COR_REFERENCIA = 3 THEN 'AMARELO'
 WHEN T.CD_COR_REFERENCIA = 4 THEN 'VERDE'
 WHEN T.CD_COR_REFERENCIA = 5 THEN 'AZUL'
 ELSE 'NÃO CLASSIFICADO'
 END AS classificacao,
 T.DS_QUEIXA_PRINCIPAL AS anamnese,
 SV.temperatura,
 SV.frequencia_cardiaca,
 SV.pressao_sistolica,
 SV.pressao_diastolica,
 SV.spo2,
 SV.glicemia,
 M.medicamentos
FROM DBAMV.ATENDIME A
JOIN DBAMV.PACIENTE P
 ON P.CD_PACIENTE = A.CD_PACIENTE
LEFT JOIN DBAMV.TRIAGEM_ATENDIMENTO T
 ON T.CD_ATENDIMENTO = A.CD_ATENDIMENTO
LEFT JOIN DBAMV.PRESTADOR PR
 ON PR.CD_PRESTADOR = A.CD_PRESTADOR
LEFT JOIN DBAMV.CID C
 ON C.CD_CID = A.CD_CID
LEFT JOIN sinais_pivot SV
 ON SV.CD_ATENDIMENTO = A.CD_ATENDIMENTO
LEFT JOIN medicamentos M
 ON M.CD_ATENDIMENTO = A.CD_ATENDIMENTO
WHERE A.TP_ATENDIMENTO = 'U'
 AND A.CD_MULTI_EMPRESA = 2
 AND A.HR_ATENDIMENTO >= TRUNC(SYSDATE, 'MM')
 AND A.HR_ATENDIMENTO < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1)
ORDER BY
 P.NM_PACIENTE,
 A.HR_ATENDIMENTO
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
""",
        "leito_uti": """
SELECT
 COUNT(l.CD_LEITO) AS leitos_ocupados_uti
FROM DBAMV.LEITO l
JOIN DBAMV.UNID_INT ui
 ON ui.CD_UNID_INT = l.CD_UNID_INT
WHERE l.DT_DESATIVACAO IS NULL
 AND ui.SN_ATIVO = 'S'
 AND l.TP_OCUPACAO = 'O'
 AND UPPER(ui.DS_UNID_INT) LIKE '%UTI 1%'
""",
        "tmp_por_setor": """
SELECT
 setor,
 tmp_dias
FROM (
 SELECT
 ui.DS_UNID_INT AS setor,
 TRUNC(AVG(a.DT_ALTA - a.DT_ATENDIMENTO), 2) AS tmp_dias
 FROM DBAMV.ATENDIME a
 JOIN DBAMV.LEITO l
 ON l.CD_LEITO = a.CD_LEITO
 JOIN DBAMV.UNID_INT ui
 ON ui.CD_UNID_INT = l.CD_UNID_INT
 WHERE a.TP_ATENDIMENTO = 'I'
 AND a.DT_ALTA IS NOT NULL
 AND a.DT_ALTA >= TRUNC(SYSDATE) - 30
 AND ui.SN_ATIVO = 'S'
 GROUP BY ui.DS_UNID_INT
)
ORDER BY tmp_dias DESC
""",
        "obitos_faixa_etaria_cid": """
SELECT
 CASE
 WHEN TRUNC(MONTHS_BETWEEN(A.DT_ALTA, P.DT_NASCIMENTO) / 12) < 18 THEN '0–17'
 WHEN TRUNC(MONTHS_BETWEEN(A.DT_ALTA, P.DT_NASCIMENTO) / 12) BETWEEN 18 AND 59 THEN '18–59'
 ELSE '60+'
 END AS faixa_etaria,
 DA.CD_CID,
 C.DS_CID AS NOME_CID,
 COUNT(*) AS qtd_obitos
FROM DBAMV.ATENDIME A
JOIN DBAMV.PACIENTE P ON P.CD_PACIENTE = A.CD_PACIENTE
JOIN DBAMV.DIAGNOSTICO_ATENDIME DA ON DA.CD_ATENDIMENTO = A.CD_ATENDIMENTO
JOIN DBAMV.CID C ON C.CD_CID = DA.CD_CID
WHERE A.SN_OBITO = 'S'
 AND A.DT_ALTA IS NOT NULL
GROUP BY
 CASE
 WHEN TRUNC(MONTHS_BETWEEN(A.DT_ALTA, P.DT_NASCIMENTO) / 12) < 18 THEN '0–17'
 WHEN TRUNC(MONTHS_BETWEEN(A.DT_ALTA, P.DT_NASCIMENTO) / 12) BETWEEN 18 AND 59 THEN '18–59'
 ELSE '60+'
 END,
 DA.CD_CID,
 C.DS_CID
""",
    },
    "Centro Cirúrgico": {
        "cirurgias_status_mes": """
SELECT TO_CHAR(DT_AVISO_CIRURGIA, 'YYYY-MM') AS MES, TP_SITUACAO, COUNT(*) AS QTDE
FROM DBAMV.AVISO_CIRURGIA
WHERE DT_AVISO_CIRURGIA IS NOT NULL
GROUP BY TO_CHAR(DT_AVISO_CIRURGIA, 'YYYY-MM'), TP_SITUACAO
ORDER BY MES, TP_SITUACAO
""",
        "cirurgias_realizadas_por_tipo": """
SELECT
 TO_CHAR(AC.DT_REALIZACAO, 'YYYY-MM') AS MES,
 C.DS_CIRURGIA AS TIPO_CIRURGIA,
 COUNT(*) AS QTDE
FROM DBAMV.AVISO_CIRURGIA AC
JOIN DBAMV.CIRURGIA_AVISO CA
 ON CA.CD_AVISO_CIRURGIA = AC.CD_AVISO_CIRURGIA
JOIN DBAMV.CIRURGIA C
 ON C.CD_CIRURGIA = CA.CD_CIRURGIA
WHERE AC.TP_SITUACAO = 'R'
 AND AC.DT_REALIZACAO IS NOT NULL
GROUP BY
 TO_CHAR(AC.DT_REALIZACAO, 'YYYY-MM'),
 C.DS_CIRURGIA
ORDER BY
 MES,
 QTDE DESC
""",
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
""",
        "top_20_medicamentos_ps": """
SELECT *
FROM (
 SELECT
 PRD.DS_PRODUTO AS MEDICAMENTO,
 COUNT(*) AS QTDE_PRESCRICOES,
 ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS RN
 FROM DBAMV.PRE_MED PM
 JOIN DBAMV.ITPRE_MED IPM ON PM.CD_PRE_MED = IPM.CD_PRE_MED
 JOIN DBAMV.PRODUTO PRD ON PRD.CD_PRODUTO = IPM.CD_PRODUTO
 JOIN DBAMV.ATENDIME ATD ON PM.CD_ATENDIMENTO = ATD.CD_ATENDIMENTO
 WHERE PM.DT_PRE_MED >= ADD_MONTHS(TRUNC(SYSDATE), -12)
 AND ATD.CD_MULTI_EMPRESA = 2
 AND IPM.CD_PRODUTO IS NOT NULL
 AND NVL(IPM.SN_CANCELADO, 'N') = 'N'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'SE %'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'CONFORME%'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'EM CASO%'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'INFUNDIR%'
 GROUP BY PRD.DS_PRODUTO
)
WHERE RN <= 20
""",
        "top_20_medicamentos_sc": """
SELECT *
FROM (
 SELECT
 PRD.DS_PRODUTO AS MEDICAMENTO,
 COUNT(*) AS QTDE_PRESCRICOES,
 ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS RN
 FROM DBAMV.PRE_MED PM
 JOIN DBAMV.ITPRE_MED IPM ON PM.CD_PRE_MED = IPM.CD_PRE_MED
 JOIN DBAMV.PRODUTO PRD ON PRD.CD_PRODUTO = IPM.CD_PRODUTO
 JOIN DBAMV.ATENDIME ATD ON PM.CD_ATENDIMENTO = ATD.CD_ATENDIMENTO
 WHERE PM.DT_PRE_MED >= ADD_MONTHS(TRUNC(SYSDATE), -12)
 AND ATD.CD_MULTI_EMPRESA = 1
 AND IPM.CD_PRODUTO IS NOT NULL
 AND NVL(IPM.SN_CANCELADO, 'N') = 'N'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'SE %'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'CONFORME%'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'EM CASO%'
 AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'INFUNDIR%'
 GROUP BY PRD.DS_PRODUTO
)
WHERE RN <= 20
""",
        "antibioticos_vs_analgesicos": """
SELECT
 TRUNC(PM.DT_PRE_MED, 'MM') AS MES,
 CASE
 WHEN UPPER(PRD.DS_PRODUTO) LIKE '%CEF%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%AMOX%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%AZITRO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CIPRO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CLINDA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%PIPER%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%METRONIDAZOL%'
 THEN 'ANTIBIOTICO'
 WHEN UPPER(PRD.DS_PRODUTO) LIKE '%DIPIRONA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%PARACETAMOL%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%MORFINA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%TRAMADOL%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CETOPROFENO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%DICLOFENACO%'
 THEN 'ANALGESICO'
 ELSE 'OUTROS'
 END AS CLASSE,
 COUNT(1) AS QTDE
FROM DBAMV.PRE_MED PM
JOIN DBAMV.ITPRE_MED IPM ON IPM.CD_PRE_MED = PM.CD_PRE_MED
JOIN DBAMV.PRODUTO PRD ON PRD.CD_PRODUTO = IPM.CD_PRODUTO
WHERE PM.DT_PRE_MED >= TRUNC(ADD_MONTHS(SYSDATE, -12))
 AND IPM.CD_PRODUTO IS NOT NULL
 AND NVL(IPM.SN_CANCELADO, 'N') = 'N'
GROUP BY
 TRUNC(PM.DT_PRE_MED, 'MM'),
 CASE
 WHEN UPPER(PRD.DS_PRODUTO) LIKE '%CEF%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%AMOX%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%AZITRO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CIPRO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CLINDA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%PIPER%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%METRONIDAZOL%'
 THEN 'ANTIBIOTICO'
 WHEN UPPER(PRD.DS_PRODUTO) LIKE '%DIPIRONA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%PARACETAMOL%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%MORFINA%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%TRAMADOL%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%CETOPROFENO%'
 OR UPPER(PRD.DS_PRODUTO) LIKE '%DICLOFENACO%'
 THEN 'ANALGESICO'
 ELSE 'OUTROS'
 END
""",
    },
 "Operações Integradas": {
        "tempo_medio_por_tipo_cirurgia": """
SELECT
    C.DS_CIRURGIA AS TIPO_CIRURGIA,
    FLOOR(AVG((AC.DT_FIM_CIRURGIA - AC.DT_INICIO_CIRURGIA) * 24)) AS HORAS,
    ROUND(MOD(AVG((AC.DT_FIM_CIRURGIA - AC.DT_INICIO_CIRURGIA) * 24) * 60, 60)) AS MINUTOS
FROM DBAMV.AVISO_CIRURGIA AC
JOIN DBAMV.CIRURGIA_AVISO CA ON CA.CD_AVISO_CIRURGIA = AC.CD_AVISO_CIRURGIA
JOIN DBAMV.CIRURGIA C ON C.CD_CIRURGIA = CA.CD_CIRURGIA
WHERE AC.TP_SITUACAO = 'R'
  AND AC.DT_INICIO_CIRURGIA IS NOT NULL
  AND AC.DT_FIM_CIRURGIA IS NOT NULL
GROUP BY C.DS_CIRURGIA
ORDER BY HORAS DESC, MINUTOS DESC
""",
        "cirurgias_realizadas_por_porte_12m": """
SELECT to_char(dt_realizacao, 'mm/yyyy') mesano,
       DECODE(cirurgia.TP_CIRURGIA, 'P', 'Pequeno Porte', 'M', 'Médio Porte', 'G', 'Grande Porte', 'E', 'Especial') porte,
       COUNT(*) qtde
FROM dbamv.aviso_cirurgia
JOIN dbamv.cirurgia_aviso ON aviso_cirurgia.cd_aviso_cirurgia = cirurgia_aviso.cd_aviso_cirurgia
JOIN dbamv.cirurgia ON cirurgia_aviso.cd_cirurgia = cirurgia.cd_cirurgia
WHERE aviso_cirurgia.tp_situacao = 'R'
  AND dt_realizacao >= Add_Months(Trunc(SYSDATE,'mm'),-12)
  AND cd_multi_empresa IN (1)
GROUP BY cirurgia.tp_cirurgia, To_Char(dt_realizacao, 'mm/yyyy')
ORDER BY To_Date(mesano, 'mm/yyyy'), 2
""",
        "cirurgias_realizadas_por_medico": """
SELECT TO_CHAR(aviso_cirurgia.dt_realizacao, 'YYYY-MM') AS mes,
       prestador.nm_prestador AS medico,
       COUNT(*) AS qtde
FROM dbamv.aviso_cirurgia
JOIN dbamv.cirurgia_aviso ON aviso_cirurgia.cd_aviso_cirurgia = cirurgia_aviso.cd_aviso_cirurgia
JOIN dbamv.prestador_aviso
  ON aviso_cirurgia.cd_aviso_cirurgia = prestador_aviso.cd_aviso_cirurgia
 AND cirurgia_aviso.cd_cirurgia_aviso = prestador_aviso.cd_cirurgia_aviso
JOIN dbamv.prestador ON prestador.cd_prestador = prestador_aviso.cd_prestador
WHERE aviso_cirurgia.tp_situacao = 'R'
  AND prestador_aviso.sn_principal = 'S'
  AND aviso_cirurgia.cd_multi_empresa = 1
GROUP BY TO_CHAR(aviso_cirurgia.dt_realizacao, 'YYYY-MM'), prestador.nm_prestador
ORDER BY TO_CHAR(aviso_cirurgia.dt_realizacao, 'YYYY-MM'), qtde DESC
""",
        "os_manutencao_aberta_ate_12h": """
SELECT CD_OS, TP_SITUACAO, DT_PEDIDO,
       TRUNC((SYSDATE - DT_PEDIDO) * 24, 2) AS horas_em_aberto,
       COALESCE(DS_SERVICO_GERAL, DS_SERVICO) AS descricao_os,
       NM_SOLICITANTE
FROM DBAMV.SOLICITACAO_OS
WHERE CD_OFICINA = 5
  AND TP_SITUACAO IN ('A','S')
  AND (SYSDATE - DT_PEDIDO) * 24 <= 12
ORDER BY horas_em_aberto DESC
""",
        "os_manutencao_aberta_12_24h": """
SELECT CD_OS, TP_SITUACAO, DT_PEDIDO,
       TRUNC((SYSDATE - DT_PEDIDO) * 24, 2) AS horas_em_aberto,
       COALESCE(DS_SERVICO_GERAL, DS_SERVICO) AS descricao_os,
       NM_SOLICITANTE
FROM DBAMV.SOLICITACAO_OS
WHERE CD_OFICINA = 5
  AND TP_SITUACAO IN ('A','S')
  AND (SYSDATE - DT_PEDIDO) * 24 > 12
  AND (SYSDATE - DT_PEDIDO) * 24 <= 24
ORDER BY horas_em_aberto DESC
""",
        "os_manutencao_aberta_mais_24h": """
SELECT CD_OS, TP_SITUACAO, DT_PEDIDO,
       TRUNC((SYSDATE - DT_PEDIDO) * 24, 2) AS horas_em_aberto,
       COALESCE(DS_SERVICO_GERAL, DS_SERVICO) AS descricao_os,
       NM_SOLICITANTE
FROM DBAMV.SOLICITACAO_OS
WHERE CD_OFICINA = 5
  AND TP_SITUACAO IN ('A','S')
  AND (SYSDATE - DT_PEDIDO) * 24 > 24
ORDER BY horas_em_aberto DESC
""",
        "medicacao_administrada": """
SELECT TO_CHAR(PM.DT_PRE_MED, 'DD/MM/YYYY') AS data_prescricao,
       PM.CD_PRE_MED,
       IPM.CD_ITPRE_MED,
       PRD.DS_PRODUTO AS medicamento,
       PR.NM_PRESTADOR AS medico
FROM DBAMV.PRE_MED PM
JOIN DBAMV.ITPRE_MED IPM ON IPM.CD_PRE_MED = PM.CD_PRE_MED
JOIN DBAMV.PRODUTO PRD ON PRD.CD_PRODUTO = IPM.CD_PRODUTO
JOIN DBAMV.PRESTADOR PR ON PR.CD_PRESTADOR = PM.CD_PRESTADOR
WHERE IPM.CD_PRODUTO IS NOT NULL
  AND NVL(IPM.SN_CANCELADO, 'N') = 'N'
  AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'SE %'
  AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'CONFORME%'
  AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'EM CASO%'
  AND UPPER(NVL(IPM.DS_ITPRE_MED, 'X')) NOT LIKE 'INFUNDIR%'
ORDER BY PM.DT_PRE_MED DESC
""",
        "medicamentos_prescritos_por_medico": """
SELECT P.NM_PRESTADOR AS medico,
       COUNT(1) AS qtde_medicamentos
FROM DBAMV.PRE_MED PM
JOIN DBAMV.ITPRE_MED IPM ON IPM.CD_PRE_MED = PM.CD_PRE_MED
JOIN DBAMV.PRESTADOR P ON P.CD_PRESTADOR = PM.CD_PRESTADOR
WHERE PM.DT_PRE_MED >= TRUNC(ADD_MONTHS(SYSDATE, -12))
  AND IPM.CD_PRODUTO IS NOT NULL
  AND NVL(IPM.SN_CANCELADO, 'N') = 'N'
GROUP BY P.NM_PRESTADOR
""",
        "atendimento_por_horario": """
SELECT to_char(triagem_atendimento.dh_pre_atendimento,'DD/MM/RRRR') data,
       to_char(triagem_atendimento.dh_pre_atendimento, 'hh24') horario,
       count(*) quantidade
FROM dbamv.triagem_atendimento
JOIN dbamv.sacr_tempo_processo
  ON sacr_tempo_processo.cd_triagem_atendimento = triagem_atendimento.cd_triagem_atendimento
WHERE cd_tipo_tempo_processo = 1
  AND trunc(triagem_atendimento.dh_pre_atendimento) = trunc(sysdate)
  AND triagem_atendimento.cd_multi_empresa in(2)
GROUP BY to_char(triagem_atendimento.dh_pre_atendimento,'DD/MM/RRRR'),
         to_char(triagem_atendimento.dh_pre_atendimento, 'hh24')
ORDER BY data DESC, horario DESC
""",
    },
}