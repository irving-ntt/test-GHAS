SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '08' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_08
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_servicio_diferente_a_03
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,21,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,21,15) end Error_en_campo_Importe_solicitado_por_el_Instituto_diferente_a_dato_numerico
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,36,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,36,8) end Error_en_campo_Fecha_de_liquidacion_diferente_a_fecha_valida_con_formato_AAAAMMDD
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,44,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,44,15) end Error_en_campo_Importe_aportaciones_aceptadas_de_rcv_a_devolver_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,15) end Error_en_campo_Importe_aportaciones_pendientes_de_rcv_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,74,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,74,15) end Error_en_campo_Importe_aportaciones_rechazadas_de_rcv_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,89,18), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,89,18) end Error_en_campo_Numero_de_aplicaciones_de_intereses_de_vivienda_solicitadas_por_el_Instituto_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,107,18), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,107,18) end Error_en_campo_Numero_de_aplicaciones_de_intereses_de_vivienda_aceptadas_a_devolver_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,125,18), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,125,18) end Error_en_campo_Numero_de_aplicaciones_de_intereses_de_vivienda_pendientes_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,143,18), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,143,18) end Error_en_campo_Numero_de_aplicaciones_de_intereses_de_vivienda_rechazadas_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,161,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,161,15) end Error_en_campo_Importe_de_vivienda_aceptadas_a_devolver_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,176,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,176,15) end Error_en_campo_Importe_de_vivienda_pendiente_a_devolver_por_la_administradora_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,191,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,191,15) end Error_en_campo_Importe_de_vivienda_rechazado_a_devolver_por_la_administradora_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'