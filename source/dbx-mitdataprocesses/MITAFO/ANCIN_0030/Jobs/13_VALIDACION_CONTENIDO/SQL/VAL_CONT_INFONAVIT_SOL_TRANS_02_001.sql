SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_registro_diferente_a_02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end Error_en_campo_Contador_de_servicio_diferente_a_dato_numerico
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL else substr(FTC_LINEA,13,2) end Error_en_campo_Tipo_entidad_receptora_de_la_cuenta_diferente_a_04
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL else substr(FTC_LINEA,15,3) end Error_en_campo_clave_de_entidad_receptora_de_la_cuenta_diferente_a_002_
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL else substr(FTC_LINEA,18,2) end Error_en_campo_tipo_de_entidad_cedente_de_la_cuenta_diferente_a_Siempre_01
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL else substr(FTC_LINEA,20,3) end Error_en_campo_Clave_entidad_cedente_de_la_cuenta_diferente_a_534
	,CASE WHEN substr(FTC_LINEA,23,2) in ('03','33') then NULL else substr(FTC_LINEA,23,2) end Error_en_campo_Origen_de_la_transferencia_diferente_a_nato_numerico_con_los_valores_03_o_33
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,25,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,25,8) end Error_en_campo_Fecha_de_presentacion_diferente_a_formato_valido_AAAAMMDD
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,11) end Error_en_campo_NSS_del_trabajador_segun_INFONAVIT_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,271,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,271,11) end Error_en_campo_NSS_del_trabajador_segun_afore_ps_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,475,15) end Error_en_campo_Numero_de_aplicaciones_de_interes_de_viv_97_de_la_ultima_aportacion_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,490,15) end Error_en_campo_Ultima_aportacion_de_viv_97_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'