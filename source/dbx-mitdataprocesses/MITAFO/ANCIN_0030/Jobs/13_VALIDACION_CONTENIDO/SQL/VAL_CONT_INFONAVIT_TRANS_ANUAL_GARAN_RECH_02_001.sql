--Plantilla: LayoutAGRechazados
--TRANSFERENCIA POR ANUALIDAD GARANTIZADA
--Subproceso: 365
--Ejemplo máscara nombre: PTCFT_20250807_TRANSAG_DEV.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_Registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) in ('03','33','43') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,23,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,23,2) end Origen_Tipo_de_la_Transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[A-Z]{7}_[A-Z]{3,4}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,CURP_del_trabajador
	--,NSS_del_trabajador_segun_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,RFC_del_trabajador_segun_INFONAVIT
	--,Apellido_paterno_del_trabajador_en_el_INFONAVIT
	--,Apellido_Materno_del_Trabajador_en_el_INFONAVIT
	--,Nombres_del_Trabajador_en_el_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,CASE WHEN substr(FTC_LINEA,240,16) = CONCAT('04002',substring(regexp_extract(#NOMBRE_ARCHIVO#,'_[0-9]{8}_[0-9]{3}_[A-Z]{6}.txt$', 0), 2, 8), substring(regexp_extract(#NOMBRE_ARCHIVO#,'.[A-Z]{1}[0-9]{8}.[A-Z]*.[A-Z]{1}[0-9]{3}.DAT$', 0), -7, 3)) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,240,16))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,240,16) end Identificador_lote_solicitud
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,NSS_segun_Afore
	--,CASE WHEN substr(FTC_LINEA,282,13) RLIKE '^[A-Za-z][A-Za-z][A-Za-z][A-Za-z][0-9][0-9][0-9][0-9][0-9][0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]$' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,282,13))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,282,13) end RFC_segun_Afore
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,Apellido_paterno_del_trabajador_en_la_Afore_cedente
	--,Apellido_materno_del_trabajador_en_la_Afore_Cedente
	--,Nombres_del_trabajador_en_la_Afore_Cedente
	--,CASE WHEN substr(FTC_LINEA,445,138) = REPEAT(' ', 138) then NULL else substr(FTC_LINEA,445,138) end Filler_06
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_de_resultado_de_la_Ope
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_proceso
	--,Nombre_del_trabajador_segun_IMSS
	--,Numero_de_credito_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,660,43) = REPEAT(' ', 43) then NULL else substr(FTC_LINEA,660,43) end Filler_07
	--,Motivo_de_la_devolucion
	--,CASE WHEN substr(FTC_LINEA,705,26) = REPEAT(' ', 26) then NULL else substr(FTC_LINEA,705,26) end Filler_08
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'