SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,3,2) = '09' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_09
	,CASE WHEN LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) IS NOT NULL THEN
		CASE WHEN CAST(substr(FTC_LINEA,5,8) AS INT) - 1 = LAG(substr(FTC_LINEA,5,8)) OVER(ORDER BY FTN_NO_LINEA) THEN NULL else substr(FTC_LINEA,5,8) end
	ELSE NULL
	END Error_en_formato_de_Consecutivo_de_registro_dentro_del_lote_invalido
	,CASE WHEN substr(FTC_LINEA,85,6) != '' then NULL else substr(FTC_LINEA,85,6) end Error_en_formato_de_Periodo_de_pago
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,91,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,91,8) end Error_en_Formato_de_fecha_de_pago_erroneo
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,99,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,99,8) end Error_en_Formato_de_fecha_valor_RCV_erroneo
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,107,11), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,107,11) end Error_en_NSS_no_valido
	,CASE WHEN substr(FTC_LINEA,142,18) != '' then NULL else substr(FTC_LINEA,142,18) end Error_en_Clave_unica_de_Registro_de_Poblacion_CURP
	,CASE WHEN substr(FTC_LINEA,280,1) in ('0','1','2',' ') then NULL else substr(FTC_LINEA,280,1) end Error_en_Indicador_de_Trabajador_con_BONO
	,CASE WHEN substr(FTC_LINEA,281,2) in ('01','02') then NULL else substr(FTC_LINEA,281,2) end Error_en_identificacion_sobre_el_tipo_de_Aportacion
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,283,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,283,7) end Error_en_Sueldo_Basico_de_Cotizacion_al_RCV_no_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,290,3), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,290,3) end Error_en_Dias_cotizados_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,293,3), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,293,3) end Error_en_Dias_de_incapacidad_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,296,3), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,296,3) end Error_en_Dias_de_ausentismo_en_el_bimestre_No_numerico
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,299,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,299,12) end Error_en_Importe_de_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,311,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,311,12) end Error_en_Importe_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,323,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,323,12) end Error_en_Importe_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,335,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,335,12) end Error_en_Importe_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,347,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,347,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Retiro_SAR_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,359,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,359,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Retiro_ISSSTE_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,371,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,371,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_CV_Aportacion_Patron_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,383,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,383,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_CV_Cuota_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,395,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,395,12) end Error_en_Importe_de_Vivienda_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,407,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,407,12) end Error_en_Importe_de_Vivienda_08_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,419,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,419,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Vivienda_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,431,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,431,12) end Error_en_Intereses_de_Pagos_Extemporaneos_de_Vivienda_08_con_formato_invalido
	,CASE WHEN substr(FTC_LINEA,443,1) in ('0','2') then NULL else substr(FTC_LINEA,443,1) end Identificador_de_Vivienda_en_Garantia
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,444,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,444,15) end Error_en_Aplicaciones_de_Intereses_del_Fondo_de_la_Vivienda_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,459,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,459,15) end Error_en_Aplicaciones_de_Intereses_del_Fondo_de_la_Vivienda_08_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,474,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,474,15) end Error_en_Aplicaciones_de_Intereses_Extemporaneos_de_Vivienda_92_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,489,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,489,15) end Error_en_Aplicaciones_de_Intereses_Extemporaneos_de_Vivienda_08_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,504,20), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,504,20) end Error_en_Valor_de_las_AIVS_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,524,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,524,12) end Error_en_formato_de_Importe_de_Cuota_Social_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,536,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,536,7) end Error_en_Importe_Aportacion_Voluntaria_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,543,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,543,7) end Error_en_Importe_Aportacion_Complementaria_de_Retiro_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,550,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,550,7) end Error_en_Importe_Aportacion_de_Ahorro_a_Largo_Plazo_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,557,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,557,7) end Error_en_Importe_de_Ahorro_Solidario_Aportacion_Trabajador_con_formato_invalido
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,564,7), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,564,7) end Error_en_Importe_de_Ahorro_Solidario_Aportacion_Dependencia_con_formato_invalido
FROM #DELTA_TABLE_NAME_001#
WHERE substr(FTC_LINEA,1,2) = '02'