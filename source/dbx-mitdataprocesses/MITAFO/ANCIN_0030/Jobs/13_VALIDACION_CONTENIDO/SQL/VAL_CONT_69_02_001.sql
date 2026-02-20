SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end Error_en_campo_Num_Cta_invdual_diferente_a_dato_numerico
  ,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,13,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,13,4) end Error_en_campo_CVE_PROCESO_FMT_diferente_a_dato_numerico
  ,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,17,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,17,4) end Error_en_campo_TMN_CVE_ITGY_FMT_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,21,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,21,10) end Error_en_campo_FTC_NSS_FMT_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'