--Plantilla: Layout de Respuesta Devolución de Pagos sin Justificacion Legal A-P v1.0 1
--Subproceso: 3832
--Ejemplo máscara nombre: PREFTD_20250828_72942_NOTDEV.TXT
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_registro
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,3,2) end Identificador_servicio
	,CASE WHEN LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,5,8) AS INT) - 1 = LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,5,8) end
	ELSE NULL
	END Consecutivo_del_lote_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,13,11), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,13,11))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,13,11) end NSS
	,CASE WHEN substr(FTC_LINEA,24,3) in ('001','002') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,24,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,24,3) end Clave_entidad_origen
	--,Registro_federal_de_contribuyentes_del_trabajador
	--,CURP
	,CASE WHEN substr(FTC_LINEA,58,50) != REPEAT(' ', 50) then NULL else substr(FTC_LINEA,58,50) end en_campo_Nombre_igual_a_vacio
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,108,6), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,108,6))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,108,6) end Periodo_de_pago
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,114,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,114,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,114,8) end Fecha_de_pago
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,122,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,122,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,122,8) end Fecha_valor_de_RCV
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,130,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,130,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,130,8) end Fecha_valor_vivienda
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,138,6), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,138,6))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,138,6) end Folio_de_pago_SUA
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,144,3), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,144,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,144,3) end Clave_entidad_receptora
	,CASE WHEN substr(FTC_LINEA,147,11) != REPEAT(' ', 11) then NULL else substr(FTC_LINEA,147,11) end Registro_patronal_IMSS
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,158,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,158,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,158,9) end Imp_Retiro_a_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,167,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,167,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,167,9) end Imp_ces_vejes_cuota_patron_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,176,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,176,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,176,9) end Imp_ces_vejes_cuota_trabajador_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,185,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,185,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,185,9) end Imp_cuota_social_devolver
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,194,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,194,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,194,9) end Imp_act_rec_retiro_devolver
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,203,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,203,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,203,9) end Imp_act_rec_ces_vejez_dev_cuota_patron
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,212,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,212,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,212,9) end Imp_act_rec_ces_vejez_dev_cuota_trab
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,221,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,221,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,221,9) end Imp_aport_patronal_viv_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,230,2), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,230,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,230,2) end dias_cotizados_bimestre_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,232,15), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,232,15))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,232,15) end apli_viv_aport_patronal_dev
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,247,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,247,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,247,9) end Imp_act_retiro_rendi_cuenta_ind
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,256,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,256,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,256,9) end Imp_act_ces_vejez_por_rendi_cuenta_ind_cuota_patron
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,265,9), '[0-9]*', '')) = 0 then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,265,9))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,265,9) end Imp_act_ces_vejez_por_rendi_cuanta_ind_cuota_trab
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,274,8), 'yyyyMMdd') IS NOT NULL then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,274,8))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,274,8) end Fecha_origen_solic_PROCESAR
	,CASE WHEN substr(FTC_LINEA,282,2) in ('01','02','04') then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,282,2))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,282,2) end Diag_devol
	,CASE WHEN substr(FTC_LINEA,282,2) in ('01','04') AND substr(FTC_LINEA,284,3) = '000' then NULL WHEN substr(FTC_LINEA,282,2) in ('02') AND substr(FTC_LINEA,284,3) != '000' then NULL WHEN substr(FTC_LINEA,284,3) = '000' then NULL WHEN LENGTH(TRIM(substr(FTC_LINEA,284,3))) = 0 then 'Registro_Vacio' else substr(FTC_LINEA,284,3) end Motivo_devolucion
	--,Resultado_de_la_operacion_1
	--,Diagnostico_1
	--,Diagnostico_2
	--,Diagnostico_3
	--,Filler
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'