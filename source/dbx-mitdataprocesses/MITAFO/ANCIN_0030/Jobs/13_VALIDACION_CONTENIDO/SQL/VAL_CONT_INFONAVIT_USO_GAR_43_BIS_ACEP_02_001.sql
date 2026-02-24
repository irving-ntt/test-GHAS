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
	END Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) = '18' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,23,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,23,2) end Origen_Tipo_de_la_Transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{9}.txt$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,33,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,33,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,33,8) end Fecha_Movimiento
	--,Curp_del_Trabajador
	--,NSS_del_trabajador_segun_Infonavit
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_01
	--,RFC_del_trabajador_segun_INFONAVIT
	--,Apellido_paterno_del_trabajador_en_el_INFONAVIT
	--,Apellido_materno_del_trabajador_en_el_INFONAVIT
	--,Nombres_del_trabajador_en_el_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_02
	--,Identificador_de_lote_de_la_solicitud
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_03
	--,NSS_segun_Afore
	--,RFC_del_trabajador_segun_afore_cedente
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_04
	--,Apellido_paterno_del_trabajador_en_la_Afore_cedente
	--,Apellido_materno_del_trabajador_en_la_AFORE_cedente
	--,Nombres_del_trabajador_en_la_AFORE_cedente
	--,CASE WHEN substr(FTC_LINEA,445,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,445,30) end Filler_05
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,475,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,475,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,475,15) end Numero_Aplicaciones_Intereses_Viv_97
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,490,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,490,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,490,15) end Saldo_Viv_97_transferido
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler_06
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_de_resultado_de_la_ope
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_proceso
	--,Nombre_del_trabajador_segun_IMSS
	--,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,650,10), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,650,10) end Numero_credito_Infonavit
	--,CASE WHEN substr(FTC_LINEA,660,53) = REPEAT(' ', 53) then NULL else substr(FTC_LINEA,660,53) end Filler_07
	--,Periodo_de_Pago
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_08
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'