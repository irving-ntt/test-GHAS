WITH RES_01 AS
(
	SELECT
		substr(FTC_LINEA,17,8) ID_08
		, substr(FTC_LINEA,25,3) ID_09
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '01'
), RES_02 AS
(
	SELECT
		SUM(CAST(substr(FTC_LINEA,181,7) AS INT) + CAST(substr(FTC_LINEA,188,7) AS INT) + CAST(substr(FTC_LINEA,195,7) AS INT) + CAST(substr(FTC_LINEA,202,7) AS INT)) ID_10
		,SUM(CAST(substr(FTC_LINEA,209,7) AS INT)) ID_12
		,SUM(CAST(substr(FTC_LINEA,216,7) AS INT)) ID_14
		,SUM(CAST(substr(FTC_LINEA,351,7) AS INT)) ID_16
		,SUM(CAST(substr(FTC_LINEA,358,7) AS INT)) ID_18
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '0' THEN CAST(substr(FTC_LINEA,223,7) AS INT) ELSE 0 END) ID_20
		,COUNT(1) ID_24
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '1' THEN CAST(substr(FTC_LINEA,223,7) AS INT) ELSE 0 END) ID_27
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '0' THEN CAST(substr(FTC_LINEA,281,15) AS INT) ELSE 0 END) ID_29
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '1' THEN CAST(substr(FTC_LINEA,281,15) AS INT) ELSE 0 END) ID_30
		,SUM(CAST(substr(FTC_LINEA,307,7) AS INT)) ID_31
		,SUM(CAST(substr(FTC_LINEA,314,15) AS INT)) ID_32
		,SUM(CAST(substr(FTC_LINEA,329,7) AS INT)) ID_33
		,SUM(CAST(substr(FTC_LINEA,336,15) AS INT)) ID_34
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '02'
), RES_08 AS
(
	SELECT
		COUNT(1) ID_25
		,SUM(
			CASE
				WHEN substr(FTC_LINEA,5,16) = '0000000000000001' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000003' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000004' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000005' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000006' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000007' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000008' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				WHEN substr(FTC_LINEA,5,16) = '0000000000000009' THEN CAST(substr(FTC_LINEA,21,15) AS DOUBLE)
				ELSE 0
			END
		) ID_26
	FROM #DELTA_TABLE_NAME_001#
	WHERE 1 = 1
		AND substr(FTC_LINEA,1,2) = '08'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end `Error_en_Tipo_de_registro_diferente_a_'09'`
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end `Error_en_Identificador_del_servicio_diferente_a_'03'`
	,CASE WHEN substr(FTC_LINEA,5,2) = '10' then NULL else substr(FTC_LINEA,5,2) end `Error_en_Identificador_de_operacion_diferente_a_'10'`
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end `Error_en_Tipo_entidad_origen_diferente_a_'03'`
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end `Error_en_Clave_entidad_origen_diferente_a__001`
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end `Error_en_Tipo_entidad_destino_diferente_a_'01'`
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end `Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_diferente_a_534`
	,CASE WHEN substr(FTC_LINEA,17,8) = RES_01.ID_08 then NULL else substr(FTC_LINEA,17,8) end `Error_en_Fecha_diferente_vs_fecha_del_encabezado_Error_en_Formato_de_fecha`
	,CASE WHEN substr(FTC_LINEA,25,3) = RES_01.ID_09 then NULL else substr(FTC_LINEA,25,3) end `Error_en_Consecutivo_erroneo_000_Error_en_Consecutivo_diferente_vs_consecutivo_del_encabezado`
	,CASE WHEN CAST(substr(FTC_LINEA,28,15) AS DOUBLE) = RES_02.ID_10 then NULL else substr(FTC_LINEA,28,15) end `Error_en_Diferencia_RCV_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,43,15) = '000000000000000' then NULL else substr(FTC_LINEA,43,15) end `Error_en_Importe_Total_Intereses_RCV_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA,58,15) AS DOUBLE) = RES_02.ID_12 then NULL else substr(FTC_LINEA,58,15) end `Error_en_Diferencia_Aportaciones_Voluntarias_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,73,15) = '000000000000000' then NULL else substr(FTC_LINEA,73,15) end `Error_en_Importe_Total_Intereses_Aportaciones_Voluntarias_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA,88,15) AS DOUBLE) = RES_02.ID_14 then NULL else substr(FTC_LINEA,88,15) end `Error_en_Diferencia_Aportaciones_Complementarias_de_Retiro_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,103,15) = '000000000000000' then NULL else substr(FTC_LINEA,103,15) end `Error_en_Importe_Total_Intereses_Aportaciones_Complementarias_de_Retiro_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA,118,15) AS DOUBLE) = RES_02.ID_16 then NULL else substr(FTC_LINEA,118,15) end `Error_en_Diferencia_Aportaciones_a_Largo_Plazo_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,133,15) = '000000000000000' then NULL else substr(FTC_LINEA,133,15) end `Error_en_Importe_Total_Intereses_Aportaciones_a_Largo_Plazo_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA,148,15) AS DOUBLE) = RES_02.ID_18 then NULL else substr(FTC_LINEA,148,15) end `Error_en_Diferencia_Aportaciones_a_una_Subcuenta_Adicional_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,163,15) = '000000000000000' then NULL else substr(FTC_LINEA,163,15) end `Error_en_Importe_Total_Intereses_Aportaciones_a_una_Subcuenta_Adicional_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA,178,15) AS DOUBLE) = RES_02.ID_20 then NULL else substr(FTC_LINEA,178,15) end `Error_en_Diferencia_Vivienda_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,193,15) = '000000000000000' then NULL else substr(FTC_LINEA,193,15) end `Error_en_Intereses_de_Vivienda_SUMARIO__diferente_a_cero`
	,CASE WHEN substr(FTC_LINEA,208,15) = '000000000000000' then NULL else substr(FTC_LINEA,208,15) end `Error_en_Importe_total_Cuotas_Gubernamentales_diferente_a_cero`
	,CASE WHEN substr(FTC_LINEA,223,15) = '000000000000000' then NULL else substr(FTC_LINEA,223,15) end `Error_en_Importe_total_intereses_Cuotas_Gubernamentales_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA, 238, 8) AS DOUBLE) = RES_02.ID_24 then NULL else substr(FTC_LINEA,238,8) end `Error_en_Diferencia_Registros_detalle_09_vs_Registros_Detalle_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 246, 6) AS DOUBLE) = RES_08.ID_25 then NULL else substr(FTC_LINEA,246,6) end `Error_en_Diferencia_Registros_detalle_09_vs_Registros_Detalle_08`
	,CASE WHEN CAST(substr(FTC_LINEA, 252, 15) AS DOUBLE) = RES_08.ID_26 then NULL else substr(FTC_LINEA,252,15) end `Error_en_Diferencia_detalle_09_vs_Importe_total_Cuenta_por_Pagar_Detalle_08`
	,CASE WHEN CAST(substr(FTC_LINEA, 267, 15) AS DOUBLE) = RES_02.ID_27 then NULL else substr(FTC_LINEA,267,15) end `Error_en_Diferencia_Vivienda_en_Garantia_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,282,15) = '000000000000000' then NULL else substr(FTC_LINEA,282,15) end `Error_en_Importe_total_de_Intereses_de_Vivienda_en_Garantia_diferente_a_cero`
	,CASE WHEN CAST(substr(FTC_LINEA, 297, 18) AS DOUBLE) = RES_02.ID_29 then NULL else substr(FTC_LINEA,297,18) end `Error_en_Diferencia_AIVS_Vivienda_97_SUMARIO_vs_DETALLE_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 315, 18) AS DOUBLE) = RES_02.ID_30 then NULL else substr(FTC_LINEA,315,18) end `Error_en_Diferencia_AIVS__Vivienda_97_en_Garantia_SUMARIO_vs_DETALLE_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 333, 15) AS DOUBLE) = RES_02.ID_31 then NULL else substr(FTC_LINEA,333,15) end `Error_en_Diferencia_Remanente_de_Vivienda_97_del_SUMARIO_vs_DETALLE_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 348, 18) AS DOUBLE) = RES_02.ID_32 then NULL else substr(FTC_LINEA,348,18) end `Error_en_Diferencia_AIVS__Vivienda_97_Remanente_del_SUMARIO_vs_DETALLE_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 366, 15) AS DOUBLE) = RES_02.ID_33 then NULL else substr(FTC_LINEA,366,15) end `Error_en_Diferencia_Int_Extemporaneos_de_Vivienda_97_del_SUMARIO_vs_DETALLE_02`
	,CASE WHEN CAST(substr(FTC_LINEA, 381, 18) AS DOUBLE) = RES_02.ID_34 then NULL else substr(FTC_LINEA,381,18) end `Error_en_Diferencia_AIVS__Vivienda_97__Int_Extemporaneos_del_SUMARIO_vs_DETALLE_02`
	,CASE WHEN substr(FTC_LINEA,399,15) = '000000000000000' then NULL else substr(FTC_LINEA,399,15) end `Error_en_Total_Intereses_en_Aclaraciones_generados_por_Pagos_Extemporaneos__de_Vivienda_97_diferente_a_cero`
FROM #DELTA_TABLE_NAME_001# RES_09
	JOIN RES_01 RES_01 ON 1 = 1
	JOIN RES_02 RES_02 ON 1 = 1
	JOIN RES_08 RES_08 ON 1 = 1
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '08'