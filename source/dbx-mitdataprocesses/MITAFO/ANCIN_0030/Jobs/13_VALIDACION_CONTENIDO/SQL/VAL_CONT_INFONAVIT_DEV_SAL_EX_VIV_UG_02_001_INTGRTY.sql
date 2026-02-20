SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_02
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,3,10) end Error_en_Cuenta_Individual_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,13,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,13,4) end Error_en_Tipo_Proceso_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,17,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,17,4) end Error_en_Tipo_de_subproceso_no_numerico
	--,CASE WHEN trim(substr(FTC_LINEA,21,11)) = '' then NULL else substr(FTC_LINEA,21, 11) end Error_en_NSS_diferente_a_vacio
	--,CASE WHEN trim(substr(FTC_LINEA,32,18)) = '' then NULL else substr(FTC_LINEA,32, 18) end Error_en_CURP_diferente_a_vacio
	,CASE WHEN substr(FTC_LINEA,50,1) in ('1','0') then NULL else substr(FTC_LINEA,50, 1) end Error_en_Resultado_de_Convivencia
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,51,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,51,4) end Error_en_Proceso_Bloqueante_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,55,4), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,55,4) end Error_en_SubProceso_Bloqueante_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,59,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,59,2) end Error_en_Estado_afil_Inicial_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,61,2), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,61,2) end Error_en_Estado_afil_Final_no_numerico
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,63,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,63,8) end Error_en_Formato_de_Fecha_de_marca
	--,CASE WHEN try_to_timestamp(substr(FTC_LINEA,71,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,71,8) end Error_en_Formato_de_Fecha_de_desmarca
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,79,50), '[0 ]*', '')) = 0 then NULL else substr(FTC_LINEA,79,50) end Error_en_Descripcion_edo_afil_bloqueante
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'