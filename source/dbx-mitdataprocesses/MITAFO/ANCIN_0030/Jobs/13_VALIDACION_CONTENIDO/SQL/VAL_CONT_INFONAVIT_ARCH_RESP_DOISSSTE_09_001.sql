WITH RES_01 AS(
	SELECT
		substr(FTC_LINEA,25,3) ID_09
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '01'
), RES_02 AS(
	SELECT
		SUM(CAST(substr(FTC_LINEA,299,12) AS INT)) ID_10
		,SUM(CAST(substr(FTC_LINEA,311,12) AS INT)) ID_11
		,SUM(CAST(substr(FTC_LINEA,323,12) AS INT)) ID_12
		,SUM(CAST(substr(FTC_LINEA,335,15) AS INT)) ID_13
		,SUM(CAST(substr(FTC_LINEA,347,12) AS INT)) ID_14
		,SUM(CAST(substr(FTC_LINEA,359,12) AS INT)) ID_15
		,SUM(CAST(substr(FTC_LINEA,371,12) AS INT)) ID_16
		,SUM(CAST(substr(FTC_LINEA,383,12) AS INT)) ID_17
		,SUM(CAST(substr(FTC_LINEA,395,12) AS INT)) ID_18
		,SUM(CAST(substr(FTC_LINEA,407,12) AS INT)) ID_19
		,SUM(CAST(substr(FTC_LINEA,419,12) AS INT)) ID_20
		,SUM(CAST(substr(FTC_LINEA,431,12) AS INT)) ID_21
		,SUM(CAST(substr(FTC_LINEA,524,12) AS INT)) ID_22
		,SUM(CAST(substr(FTC_LINEA,536,7) AS INT)) ID_23
		,SUM(CAST(substr(FTC_LINEA,543,7) AS INT)) ID_24
		,SUM(CAST(substr(FTC_LINEA,550,7) AS INT)) ID_25
		,SUM(CAST(substr(FTC_LINEA,557,7) AS INT)) ID_26
		,SUM(CAST(substr(FTC_LINEA,564,7) AS INT)) ID_27
		,COUNT(1) ID_28
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,3,2) = '09' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,5,2) = '70' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_de_operacion_diferente_a_70
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_entidad_origen_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_entidad_origen_diferente_a_534
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_entidad_destino_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_diferente_a_001
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,17,8) end Error_en_Fecha_diferente_vs_fecha_del_encabezado
	,CASE WHEN CAST(substr(FTC_LINEA, 25, 3) AS DOUBLE) = RES_01.ID_09 then NULL else substr(FTC_LINEA, 25, 3) end Error_en_Consecutivo_diferente_vs_consecutivo_del_encabezado
	,CASE WHEN CAST(substr(FTC_LINEA, 28, 15) AS DOUBLE) = RES_02.ID_10 then NULL else substr(FTC_LINEA, 28, 15) end Error_en_Importe_SAR_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 43, 12) AS DOUBLE) = RES_02.ID_11 then NULL else substr(FTC_LINEA, 43, 12) end Error_en_Importe_Retiro_ISSSTE_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 55, 12) AS DOUBLE) = RES_02.ID_12 then NULL else substr(FTC_LINEA, 55, 12) end Error_en_Importe_CV_Aportacion_Patron_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 70, 15) AS DOUBLE) = RES_02.ID_13 then NULL else substr(FTC_LINEA, 70, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 85, 12) AS DOUBLE) = RES_02.ID_14 then NULL else substr(FTC_LINEA, 85, 12) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_00
	,CASE WHEN CAST(substr(FTC_LINEA, 97, 15) AS DOUBLE) = RES_02.ID_15 then NULL else substr(FTC_LINEA, 97, 15) end Error_en_Importe_Total_de_Intereses_de_Pagos_Extemporaneos_de_Retiro_ISSSTE_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 112, 15) AS DOUBLE) = RES_02.ID_16 then NULL else substr(FTC_LINEA, 112, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_01
	,CASE WHEN CAST(substr(FTC_LINEA, 127, 15) AS DOUBLE) = RES_02.ID_17 then NULL else substr(FTC_LINEA, 127, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_02
	,CASE WHEN CAST(substr(FTC_LINEA, 142, 15) AS DOUBLE) = RES_02.ID_18 then NULL else substr(FTC_LINEA, 142, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_03
	,CASE WHEN CAST(substr(FTC_LINEA, 157, 15) AS DOUBLE) = RES_02.ID_19 then NULL else substr(FTC_LINEA, 157, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_04
	,CASE WHEN CAST(substr(FTC_LINEA, 172, 15) AS DOUBLE) = RES_02.ID_20 then NULL else substr(FTC_LINEA, 172, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_05
	,CASE WHEN CAST(substr(FTC_LINEA, 187, 15) AS DOUBLE) = RES_02.ID_21 then NULL else substr(FTC_LINEA, 187, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_06
	,CASE WHEN CAST(substr(FTC_LINEA, 202, 15) AS DOUBLE) = RES_02.ID_22 then NULL else substr(FTC_LINEA, 202, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_07
	,CASE WHEN CAST(substr(FTC_LINEA, 217, 15) AS DOUBLE) = RES_02.ID_23 then NULL else substr(FTC_LINEA, 217, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_08
	,CASE WHEN CAST(substr(FTC_LINEA, 232, 15) AS DOUBLE) = RES_02.ID_24 then NULL else substr(FTC_LINEA, 232, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_09
	,CASE WHEN CAST(substr(FTC_LINEA, 247, 15) AS DOUBLE) = RES_02.ID_25 then NULL else substr(FTC_LINEA, 247, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_10
	,CASE WHEN CAST(substr(FTC_LINEA, 262, 15) AS DOUBLE) = RES_02.ID_26 then NULL else substr(FTC_LINEA, 262, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_11
	,CASE WHEN CAST(substr(FTC_LINEA, 277, 15) AS DOUBLE) = RES_02.ID_27 then NULL else substr(FTC_LINEA, 277, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_12
	,CASE WHEN CAST(substr(FTC_LINEA, 292, 9) AS DOUBLE) = RES_02.ID_28 then NULL else substr(FTC_LINEA, 292, 9) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_13
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01
		ON 1 = 1
	JOIN RES_02
		ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'