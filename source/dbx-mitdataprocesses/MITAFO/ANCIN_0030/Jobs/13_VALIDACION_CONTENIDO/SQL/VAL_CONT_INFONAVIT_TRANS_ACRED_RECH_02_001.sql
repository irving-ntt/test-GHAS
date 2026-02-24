--Plantilla: LayoutTARechazados
--TRANSFERENCIAS DE ACREDITADOS INFONAVIT
--Subproceso: 364
--Ejemplo máscara nombre: PTCFTA_20250807_001_TRADEV.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_Registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Contador_de_Servicio
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
	--,Origen_Tipo_de_la_Transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z]{6}.TXT', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_Presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler
	--,CURP_del_trabajador
	--,NSS_del_trabajador_segun_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler
	--,RFC_del_trabajador_segun_INFONAVIT
	--,Apellido_paterno_del_trabajador_en_el_INFONAVIT
	--,Apellido_Materno_del_Trabajador_en_el_INFONAVIT
	--,Nombres_del_Trabajador_en_el_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler
	--,Identificador_de_lote_en_la_solicitud
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filller
	--,NSS_segun_Afore
	--,RFC_segun_Afore
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler
	--,Apellido_paterno_del_trabajador_en_la_Afore_cedente
	--,Apellido_materno_del_trabajador_en_la_Afore_Cedente
	--,Nombres_del_trabajador_en_la_Afore_Cedente
	--,CASE WHEN substr(FTC_LINEA,445,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,445,30) end Filler
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,475,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,475,15) end Numero_de_Aplicaciones_de_Intereses_de_Vivienda_97_de_la_ultima_aportacion
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,490,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,490,15) end Ultima_aportacion_Vivienda_97
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_de_Resultado_de_la_Operacion
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_Proceso
	--,Nombre_del_trabajador_segun_IMSS
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,650,10), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,650,10))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,650,10) end Numero_de_Credito_Infonavit
	--,CASE WHEN substr(FTC_LINEA,660,43) = REPEAT(' ', 43) then NULL else substr(FTC_LINEA,660,43) end Filler
	--,Motivo_de_Devolucion
	--,CASE WHEN substr(FTC_LINEA,705,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,705,8) end Filler
	--,Periodo_de_pago
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'