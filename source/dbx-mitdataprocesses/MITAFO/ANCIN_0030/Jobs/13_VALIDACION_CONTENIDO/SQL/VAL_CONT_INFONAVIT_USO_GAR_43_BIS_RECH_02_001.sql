--Plantilla: LayoutUGAceptados
--USO DE GARANTÍA POR 43 BIS
--Subproceso: 368
--Ejemplo máscara nombre: PTCFTA_20250807_001_DEVUSOG43.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
  ,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_entidad_receptora_de_la_cuenta
  ,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
  ,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
  ,CASE WHEN substr(FTC_LINEA,23,2) = '18' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,23,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,23,2) end Origen_Tipo_de_la_Transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substr(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{9,12}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,CURP
	--,NSS
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,RFC
	--,APELLIDO PATERNO
	--,APELLIDO MATERNO
	--,NOMBRE
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,IDENTIFICADOR LOTE
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,NSS
	--,RFC
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,APELLIDO PATERNO AFORE
	--,APELLIDO MATERNO AFORE
	--,NOMBRE AFORE
	--,CASE WHEN substr(FTC_LINEA,445,45) = REPEAT(' ', 45) then NULL else substr(FTC_LINEA,445,45) end Filler_06
	,CASE WHEN substr(FTC_LINEA,490,15) = REPEAT('0', 15) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,490,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,490,15) END Enviar_ceros
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler_07
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_resultado_de_la_Op
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_Proceso
	--,NOMBRE IMSS
	,CASE WHEN substr(FTC_LINEA,650,10) = REPEAT('0', 10) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,650,10))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,650,10) END Numero_Credito_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,660,43) = REPEAT(' ', 43) then NULL else substr(FTC_LINEA,660,43) end Filler_08
	--,MOTIVO DEVOLUCION
	--,CASE WHEN substr(FTC_LINEA,705,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,705,8) end Filler_09
	--,PERIODO PAGO
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_10
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'