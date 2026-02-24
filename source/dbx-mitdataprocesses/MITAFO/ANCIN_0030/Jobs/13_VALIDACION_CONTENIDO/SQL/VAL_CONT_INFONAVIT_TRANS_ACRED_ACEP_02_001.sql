--Plantilla: LayoutTAAceptados
--TRANSFERENCIAS DE ACREDITADOS INFONAVIT
--Subproceso: 364
--Ejemplo máscara nombre: PTCFTA_20250807_001_TRADEV.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_entidad_ced_de_la_cuenta
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z]{6}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,CASE WHEN substr(FTC_LINEA,445,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,445,30) end Filler_06
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,475,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,475,15) END Num_Aplicaciones_Intereses_Viv_97
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,490,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,490,15) END Ultima_aportacion_Viv_97
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler_07
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_Resultado_de_la_Op
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_Proceso
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,650,10), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,650,10))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,650,10) end Numero_Credito_Infonavit
	--,CASE WHEN substr(FTC_LINEA,660,43) = REPEAT(' ', 43) then NULL else substr(FTC_LINEA,660,43) end Filler_07
	--,CASE WHEN substr(FTC_LINEA,705,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,705,8) end Filler_08
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_09
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'