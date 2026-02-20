SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_09
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,9), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,9) end Error_en_campo_Cantidad_de_registros_de_Detalle_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,42,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,42,15) end Error_en_campo_Suma_de_numero_de_aplicaciones_de_intereses_de_vivienda_97_de_la_ultima_aportacion_suma_de_registros_del_detalle_02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,57,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,57,15) end Error_en_campo_Suma_de_la_ultima_aportacion_de_vivienda_97_diferente_a_suma_de_registros_del_detalle_02
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'