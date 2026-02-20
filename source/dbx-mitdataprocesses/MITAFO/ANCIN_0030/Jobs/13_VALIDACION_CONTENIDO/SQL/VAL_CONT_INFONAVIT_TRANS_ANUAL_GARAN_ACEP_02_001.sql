--Plantilla: LayoutAGAceptados
--TRANSFERENCIA POR ANUALIDAD GARANTIZADA
--Subproceso: 365
--Ejemplo máscara nombre: PTCFT_20250807_TRANSAG_DEV.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
	--,Origen_tipo_de_la_transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[A-Z]{7}_[A-Z]{3,4}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,Curp_del_trabajador
	--,NSS_del_trabajador_segun_Infonavit
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,RFC_del_trabajador_segun_Infonavit
	--,Apellido_paterno_del_trabajador_en_el_Infonavit
	--,Apellido_materno_del_trabajador_en_el_Infonavit
	--,Nombres_del_trabajador_en_el_Infonavit
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,CASE WHEN substr(FTC_LINEA,240,16) = CONCAT('04002',substring(regexp_extract(#NOMBRE_ARCHIVO#,'_[0-9]{8}_[0-9]{3}_[A-Z]{6}.txt$', 0), 2, 8), substring(regexp_extract(#NOMBRE_ARCHIVO#,'.[A-Z]{1}[0-9]{8}.[A-Z]*.[A-Z]{1}[0-9]{3}.DAT$', 0), -7, 3)) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,240,16))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,240,16) end Identificador_lote_solicitud
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,NSS_segun_Afore
	--,RFC_del_trabajador_segun_afore_cedente
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,Apellido_paterno_del_trabajador_en_la_afore_Cedente
	--,Apellido_materno_del_trabajador_en_la_afore_Cedente
	--,Nombres_del_trabajador_en_la_afore_Cedente
	--,CASE WHEN substr(FTC_LINEA,445,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,445,30) end Filler_06
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,475,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,475,15) end Error_Numero_aplicaciones_Intereses_Viv_92
	,CASE WHEN substr(FTC_LINEA,490,2) in ('01','  ') then NULL else substr(FTC_LINEA,490,2) end Indicador_diferencia_Viv_92
	--,CASE WHEN substr(FTC_LINEA,492,58) = REPEAT(' ', 58) then NULL else substr(FTC_LINEA,492,58) end Filler_07
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,550,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,550,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,550,15) end Numero_Aplicaciones_Intereses_Viv_97_Aceptados
	,CASE WHEN substr(FTC_LINEA,565,2) in ('01','  ') then NULL else substr(FTC_LINEA,565,2) end Indicador_diferencia_Viv_97
	--,CASE WHEN substr(FTC_LINEA,567,16) = REPEAT(' ', 16) then NULL else substr(FTC_LINEA,567,16) end Filler_08
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_de_resultado_de_la_ope
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_proceso
	--,Nombre_del_trabajador_segun_IMSS
	--,Numero_de_credito_Infonavit
	--,CASE WHEN substr(FTC_LINEA,660,71) = REPEAT(' ', 71) then NULL else substr(FTC_LINEA,660,71) end Filler_09
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'