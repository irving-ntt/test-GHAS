
WITH RES_01 AS(
	SELECT
		substr(FTC_LINEA,25,3) ID_09
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '01'
),
RES_02 AS(
	SELECT
		SUM(CAST(substr(FTC_LINEA,181,7) AS INT) + CAST(substr(FTC_LINEA,188,7) AS INT) + CAST(substr(FTC_LINEA,195,7) AS INT) + CAST(substr(FTC_LINEA,202,7) AS INT)) ID_10
		,SUM(CAST(substr(FTC_LINEA,209,7) AS INT)) ID_12
		,SUM(CAST(substr(FTC_LINEA,216,7) AS INT)) ID_14
		,SUM(CAST(substr(FTC_LINEA,351,7) AS INT)) ID_16
		,SUM(CAST(substr(FTC_LINEA,358,7) AS INT)) ID_18
		,SUM(CAST(substr(FTC_LINEA,223,7) AS INT)) ID_20
		,COUNT(1) ID_24
		,SUM(CAST(substr(FTC_LINEA,181,7) AS INT) + CAST(substr(FTC_LINEA,188,7) AS INT) + CAST(substr(FTC_LINEA,195,7) AS INT) + CAST(substr(FTC_LINEA,202,7) AS INT) + CAST(substr(FTC_LINEA,209,7) AS INT) + CAST(substr(FTC_LINEA,216,7) AS INT) + CAST(substr(FTC_LINEA,223,7) AS INT) + CAST(substr(FTC_LINEA,307,7) AS INT) + CAST(substr(FTC_LINEA,329,7) AS INT) + CAST(substr(FTC_LINEA,351,7) AS INT) + CAST(substr(FTC_LINEA,358,7) AS INT)) ID_26
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '2' THEN CAST(substr(FTC_LINEA,223,7) AS INT) ELSE 0 END) ID_27
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '0' THEN CAST(substr(FTC_LINEA,281,15) AS INT) ELSE 0 END) ID_29
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '2' THEN CAST(substr(FTC_LINEA,281,15) AS INT) ELSE 0 END) ID_30
		,SUM(CAST(substr(FTC_LINEA,307,7) AS INT)) ID_31
		,SUM(CAST(substr(FTC_LINEA,314,15) AS INT)) ID_32
		,SUM(CAST(substr(FTC_LINEA,329,7) AS INT)) ID_33
		,SUM(CAST(substr(FTC_LINEA,336,15) AS INT)) ID_34
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
),
RES_03 AS(
	SELECT
		SUM(CAST(substr(FTC_LINEA,181,7) AS INT) + CAST(substr(FTC_LINEA,188,7) AS INT) + CAST(substr(FTC_LINEA,195,7) AS INT) + CAST(substr(FTC_LINEA,202,7) AS INT)) ID_11
		,SUM(CAST(substr(FTC_LINEA,209,7) AS INT)) ID_13
		,SUM(CAST(substr(FTC_LINEA,216,7) AS INT)) ID_15
		,SUM(CAST(substr(FTC_LINEA,288,7) AS INT)) ID_17
		,SUM(CAST(substr(FTC_LINEA,295,7) AS INT)) ID_19
		,SUM(CAST(substr(FTC_LINEA,223,7) AS INT)) ID_21
		,COUNT(1) ID_24
		,SUM(CAST(substr(FTC_LINEA,181,7) AS INT) + CAST(substr(FTC_LINEA,188,7) AS INT) + CAST(substr(FTC_LINEA,195,7) AS INT) + CAST(substr(FTC_LINEA,202,7) AS INT) + CAST(substr(FTC_LINEA,209,7) AS INT) + CAST(substr(FTC_LINEA,216,7) AS INT) + CAST(substr(FTC_LINEA,288,7) AS INT) + CAST(substr(FTC_LINEA,295,7) AS INT)) ID_26
		,SUM(CASE WHEN substr(FTC_LINEA,280,1) = '2' THEN CAST(substr(FTC_LINEA,223,7) AS INT) ELSE 0 END) ID_28
		,SUM(CAST(substr(FTC_LINEA,281,15) AS INT)) ID_35
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,3,2) = '03' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,5,2) = '10' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_de_operacion_diferente_a_10
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_entidad_origen_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_entidad_origen_diferente_a_534
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_entidad_destino_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_diferente_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_Fecha_diferente_vs_fecha_del_encabezado
	,CASE WHEN CAST(substr(FTC_LINEA, 25, 3) AS DOUBLE) = RES_01.ID_09 then NULL else substr(FTC_LINEA, 25, 3) end Error_en_Consecutivo_erroneo_000_Error_en_Consecutivo_diferente_vs_consecutivo_del_encabezado
	,CASE WHEN CAST(substr(FTC_LINEA, 28, 15) AS DOUBLE) = RES_02.ID_10 then NULL else substr(FTC_LINEA, 28, 15) end Error_en_Diferencia_RCV_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 43, 15) AS DOUBLE) = RES_03.ID_11 then NULL else substr(FTC_LINEA, 43, 15) end Error_en_Importe_Total_Intereses_RCV_Diferencia_RCV_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 58, 15) AS DOUBLE) = RES_02.ID_12 then NULL else substr(FTC_LINEA, 58, 15) end Error_en_Diferencia_Aportaciones_Voluntarias_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 73, 15) AS DOUBLE) = RES_03.ID_13 then NULL else substr(FTC_LINEA, 73, 15) end Error_en_Importe_Total_Intereses_Aportaciones_Voluntarias_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 88, 15) AS DOUBLE) = RES_02.ID_14 then NULL else substr(FTC_LINEA, 88, 15) end Error_en_Diferencia_Aportaciones_Complementarias_de_Retiro_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 103, 15) AS DOUBLE) = RES_03.ID_15 then NULL else substr(FTC_LINEA, 103, 15) end Error_en_Importe_Total_Intereses_Aportaciones_Complementarias_de_Retiro_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 118, 15) AS DOUBLE) = RES_02.ID_16 then NULL else substr(FTC_LINEA, 118, 15) end Error_en_Diferencia_Aportaciones_a_Largo_Plazo_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 133, 15) AS DOUBLE) = RES_03.ID_17 then NULL else substr(FTC_LINEA, 133, 15) end Error_en_Importe_Total_Intereses_Aportaciones_a_Largo_Plazo_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 148, 15) AS DOUBLE) = RES_02.ID_18 then NULL else substr(FTC_LINEA, 148, 15) end Error_en_Diferencia_Aportaciones_a_una_Subcuenta_Adicional_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 163, 15) AS DOUBLE) = RES_03.ID_19 then NULL else substr(FTC_LINEA, 163, 15) end Error_en_Importe_Total_Intereses_Aportaciones_a_una_Subcuenta_Adicional_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 178, 15) AS DOUBLE) = RES_02.ID_20 then NULL else substr(FTC_LINEA, 178, 15) end Error_en_Diferencia_Vivienda_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 193, 15) AS DOUBLE) = RES_03.ID_21 then NULL else substr(FTC_LINEA, 193, 15) end Error_en_Diferencia_Interes_Vivienda_SUMARIO_vs_DETALLE_03
	,CASE WHEN substr(FTC_LINEA,208,15) = '000000000000000' then NULL else substr(FTC_LINEA,208,15) end Error_en_Importe_total_Cuotas_Gubernamentales_diferente_a_cero
	,CASE WHEN substr(FTC_LINEA,223,15) = '000000000000000' then NULL else substr(FTC_LINEA,223,15) end Error_en_Importe_total_intereses_Cuotas_Gubernamentales_diferente_a_cero
	,CASE WHEN CAST(substr(FTC_LINEA, 238, 8) AS DOUBLE) = RES_02.ID_24 then NULL else substr(FTC_LINEA, 238, 8) end Error_en_Diferencia_Registros_detalle_09_vs_Registros_Detalle_02
	,CASE WHEN CAST(substr(FTC_LINEA, 252, 15) AS DOUBLE) = (RES_02.ID_26 + RES_03.ID_26) then NULL else substr(FTC_LINEA, 252, 8) end Error_en_Importe_total_vs_Sumatoria_importe_de_Aportaciones_e_Intereses_Aceptadas
	,CASE WHEN CAST(substr(FTC_LINEA, 267, 15) AS DOUBLE) = RES_02.ID_27 then NULL else substr(FTC_LINEA, 267, 8) end Error_en_Diferencia_Vivienda_en_Garantia_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 282, 15) AS DOUBLE) = RES_03.ID_28 then NULL else substr(FTC_LINEA, 282, 15) end Error_en_Diferencia_Vivienda_en_Garantia_SUMARIO_vs_DETALLE_03
	,CASE WHEN CAST(substr(FTC_LINEA, 297, 18) AS DOUBLE) = RES_02.ID_29 then NULL else substr(FTC_LINEA, 297, 18) end Error_en_Diferencia_AIVS_Vivienda_97_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 315, 18) AS DOUBLE) = RES_02.ID_30 then NULL else substr(FTC_LINEA, 315, 18) end Error_en_Diferencia_AIVS_97_Vivienda_en_Garantia_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 333, 15) AS DOUBLE) = RES_02.ID_31 then NULL else substr(FTC_LINEA, 333, 15) end Error_en_Diferencia_Remanente_de_Vivienda_97_del_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 348, 18) AS DOUBLE) = RES_02.ID_32 then NULL else substr(FTC_LINEA, 348, 18) end Error_en_Diferencia_AIVS_Vivienda_97_Remanente_del_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 366, 15) AS DOUBLE) = RES_02.ID_33 then NULL else substr(FTC_LINEA, 366, 15) end Error_en_Diferencia_Int_Extemporaneos_de_Vivienda_del_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 381, 18) AS DOUBLE) = RES_02.ID_34 then NULL else substr(FTC_LINEA, 381, 18) end Error_en_Diferencia_AIVS_Vivienda_97_Int_Extemporaneos_del_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 399, 15) AS DOUBLE) = RES_03.ID_35 then NULL else substr(FTC_LINEA, 399, 15) end Error_en_Importe_diferente_a_cero
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01
		ON 1 = 1
	JOIN RES_02
		ON 1 = 1
	JOIN RES_03
		ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'