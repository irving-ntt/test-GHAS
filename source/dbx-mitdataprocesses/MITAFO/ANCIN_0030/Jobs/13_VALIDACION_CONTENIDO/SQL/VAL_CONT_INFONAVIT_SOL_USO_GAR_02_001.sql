SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end Error_en_campo_Contador_de_servicio_diferente_a_dato_numerico
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL else substr(FTC_LINEA,13,2) end Error_en_campo_Tipo_de_entidad_receptora_de_la_cuenta_diferente_04
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL else substr(FTC_LINEA,15,3) end Error_en_campo_Clave_de_entidad_receptora_de_la_cuenta_diferente_a_002
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL else substr(FTC_LINEA,18,2) end Error_en_campo_Tipo_de_entidad_cedente_de_la_cuenta_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL else substr(FTC_LINEA,20,3) end Error_en_campo_Clave_de_la_entidad_cedente_de_la_cuenta_diferente_a_Siempre_534
	,CASE WHEN substr(FTC_LINEA,23,2) = '18' then NULL else substr(FTC_LINEA,26,-1) end Error_en_campo_Origen_Tipo_de_la_Transferencia_diferente_a_18_Cuenta_con_credito_43_bis --
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,25,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,25,8) end Error_en_campo_Fecha_de_presentacion_diferente_a_formato_valido_AAAAMMDD
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,33,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,33,8) end Error_en_campo_Fecha_de_Movimiento_diferente_a_formato_valido_AAAAMMDD
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,11) end Error_en_campo_NSS_del_trabajador_segun_INFONAVIT_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,271,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,271,11) end Error_en_campo_NSS_segun_Afore_diferente_a_dato_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,475,15) end Error_en_campo_Numero_de_Aplicaciones_de_Intereses_de_Vivienda_97_solicitado_diferente_a_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,490,15) end Error_en_campo_Saldo_de_Vivienda_97_solicitado_diferente_a_dato_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'