--Plantilla: Layout de Notificación de Saldo (Aceptados)
--SOLICITUD DE MARCA DE CUENTAS POR 43 BIS
--Subproceso: 354
--Ejemplo máscara nombre: PTCFTA_AAAAMMDD_CCC_ACEPMARCA43.TXT
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
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) = '16' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,23,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,23,2) end Origen_Tipo_de_la_transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{11}.TXT$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,Curp_del_Trabajador
	--,NSS_del_trabajador_segun_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,RFC_del_trabajador_segun_INFONAVIT
	--,Apellido_paterno_del_trabajador_en_el_INFONAVIT
	--,Apellido_materno_del_trabajador_en_el_INFONAVIT
	--,Nombres_del_trabajador_en_el_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,Identificador_de_lote_de_la_solicitud
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,NSS_segun_AFORE
	--,RFC_del_trabajador_segun_AFORE_Cedente
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,Apellido_paterno_del_trabajador_en_la_Afore_cedente
	--,Apellido_Materno_del_trabajador_en_la_AFORE_Cedente
	--,Nombres_del_trabajador_en_la_AFORE_Cedente
	--,CASE WHEN substr(FTC_LINEA,445,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,445,30) end Filler_06
	--,Numero_de_Aplicaciones_de_Vivienda_97
	--,Saldo_de_Vivienda_97
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL else substr(FTC_LINEA,505,78) end Filler_07
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Error_en_Codigo_de_resultado_de_la_Ope
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Error_en_Motivo_de_rechazo_de_lote
	--,Nombre_del_trabajador_segun_el_IMSS
	--,Numero_de_credito_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,660,53) = REPEAT(' ', 53) then NULL else substr(FTC_LINEA,660,53) end Filler_08
	--,CASE WHEN substr(FTC_LINEA,713,6) = REPEAT(' ', 6) then NULL else substr(FTC_LINEA,713,6) end Inconsistencia_en_Periodo_de_Pago
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_09
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'