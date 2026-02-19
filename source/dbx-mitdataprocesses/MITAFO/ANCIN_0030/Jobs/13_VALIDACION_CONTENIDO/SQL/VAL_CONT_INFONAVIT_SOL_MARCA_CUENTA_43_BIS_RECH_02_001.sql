--Plantilla: LayoutRespuestaMarca
--SOLICITUD DE MARCA DE CUENTAS POR 43 BIS
--Subproceso: 354
--Ejemplo máscara nombre: PTCFTA_20250801_001_DEVMARCA43.txt
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_de_registro
	,CASE WHEN LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,3,10) AS INT) - 1 = LAG(substr(FTC_LINEA,3,10)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,3,10) end
	ELSE NULL
	END Error_Consecutivo_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,13,2) = '04' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,2) end Tipo_de_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,15,3) = '002' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,15,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,15,3) end Clave_de_la_entidad_receptora_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,18,2) = '01' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,18,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,18,2) end Tipo_de_entidad_cedente_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,20,3) = '534' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,20,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,20,3) end Clave_de_entidad_ced_de_la_cuenta
	,CASE WHEN substr(FTC_LINEA,23,2) = '16' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,23,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,23,2) end Origen_Tipo_de_transferencia
	--,CASE WHEN substr(FTC_LINEA,25,8) = substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{10}.txt$', 0), 2, 8) THEN NULL else substr(FTC_LINEA,25,8) end Error_Fecha_de_presentacion
	--,CASE WHEN substr(FTC_LINEA,33,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,33,8) end Filler_01
	--,Curp_del_Trabajador
	--,NSS_del_trabajador_segun_INFONAVIT
	--,CASE WHEN substr(FTC_LINEA,70,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,70,15) end Filler_02
	--,RFC_del_trabajador_segun_Infonavit
	--,Apellido_paterno_del_trabajador_en_el_Infonavit
	--,Apellido_materno_del_trabajador_en_el_Infonavit
	--,Nombres_del_trabajador_en_el_Infonavit
	--,CASE WHEN substr(FTC_LINEA,218,22) = REPEAT(' ', 22) then NULL else substr(FTC_LINEA,218,22) end Filler_03
	--,CASE WHEN substr(FTC_LINEA,240,16) = CONCAT('04002',substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{10}.TXT$', 0), 2, 8), substring(regexp_extract('#NOMBRE_ARCHIVO#','_[0-9]{8}_[0-9]{3}_[A-Z0-9]{10}.txt$', 0), 11, 3)) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,240,16))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,240,16) end Siempre_16_Indica_cuenta_con_credito_43_bis
	--,CASE WHEN substr(FTC_LINEA,256,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,256,15) end Filler_04
	--,NSS_segun_Afore
	--,RFC_segun_Afore
	--,CASE WHEN substr(FTC_LINEA,295,30) = REPEAT(' ', 30) then NULL else substr(FTC_LINEA,295,30) end Filler_05
	--,Apellido_paterno_del_trabajador_en_la_Afore_cedente
	--,Apellido_materno_del_trabajador_en_la_Afore_cedente
	--,Nombres_del_trabajador_en_la_Afore_cedente
	--,CASE WHEN substr(FTC_LINEA,445,45) = REPEAT(' ', 45) then NULL else substr(FTC_LINEA,445,45) end Filler_06
	,CASE WHEN substr(FTC_LINEA,490,15) = REPEAT('0', 15) then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,490,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,490,15) END Saldo_de_Vivienda_97
	--,CASE WHEN substr(FTC_LINEA,505,78) = REPEAT(' ', 78) then NULL substr(FTC_LINEA,505,78) end Filler_07
	--,CASE WHEN substr(FTC_LINEA,583,2) = REPEAT(' ', 2) then NULL else substr(FTC_LINEA,583,2) end Codigo_de_resultado_de_la_operacion
	--,CASE WHEN substr(FTC_LINEA,585,15) = REPEAT(' ', 15) then NULL else substr(FTC_LINEA,585,15) end Diagnostico_del_proceso
	--,Nombre_del_trabajador_segun_IMSS
	--,Numero_de_Credito_Infonavit
	--,CASE WHEN substr(FTC_LINEA,660,43) = REPEAT(' ', 43) then NULL else substr(FTC_LINEA,660,43) end Filler_08
	,CASE WHEN substr(FTC_LINEA,703,2) in ('04','08','11','16','19','20', '00','01') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,703,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,703,2) end Motivo_Devolucion
	--,CASE WHEN substr(FTC_LINEA,705,8) = REPEAT(' ', 8) then NULL else substr(FTC_LINEA,705,8) end Filler_09
	--,CASE WHEN substr(FTC_LINEA,713,6) = REPEAT(' ', 6) then NULL else substr(FTC_LINEA,713,6) end Periodo_de_Pago
	--,CASE WHEN substr(FTC_LINEA,719,12) = REPEAT(' ', 12) then NULL else substr(FTC_LINEA,719,12) end Filler_10
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'
