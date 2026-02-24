WITH RES_01 AS(
	SELECT
		TO_DATE(substr(FTC_LINEA,17,8), 'yyyyMMdd') ID_08
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '01'
),
RES_02 AS(
	SELECT
		SUM(CAST(substr(FTC_LINEA,290,12) AS INT)) ID_23
		,SUM(CAST(substr(FTC_LINEA,302,12) AS INT)) ID_24
		,SUM(CAST(substr(FTC_LINEA,314,12) AS INT)) ID_25
		,SUM(CAST(substr(FTC_LINEA,326,12) AS INT)) ID_26
		,SUM(CAST(substr(FTC_LINEA,338,12) AS INT)) ID_27
		,SUM(CAST(substr(FTC_LINEA,350,12) AS INT)) ID_28
		,SUM(CAST(substr(FTC_LINEA,362,12) AS INT)) ID_29
		,SUM(CAST(substr(FTC_LINEA,374,12) AS INT)) ID_30
		,SUM(CAST(substr(FTC_LINEA,386,7) AS INT)) ID_31
		,SUM(CAST(substr(FTC_LINEA,393,7) AS INT)) ID_32
		,SUM(CAST(substr(FTC_LINEA,400,12) AS INT)) ID_33
		,SUM(CAST(substr(FTC_LINEA,412,12) AS INT)) ID_34
		,SUM(CAST(substr(FTC_LINEA,424,12) AS INT)) ID_35
		,SUM(CAST(substr(FTC_LINEA,436,12) AS INT)) ID_36
		,SUM(CAST(substr(FTC_LINEA,448,12) AS INT)) ID_37
		,SUM(CAST(substr(FTC_LINEA,460,12) AS INT)) ID_38
		,SUM(CAST(substr(FTC_LINEA,472,12) AS INT)) ID_39
		,SUM(CAST(substr(FTC_LINEA,484,12) AS INT)) ID_40
		,SUM(CAST(substr(FTC_LINEA,496,12) AS INT)) ID_41
		,SUM(CAST(substr(FTC_LINEA,508,12) AS INT)) ID_42
		,SUM(CAST(substr(FTC_LINEA,520,12) AS INT)) ID_43
		,SUM(CAST(substr(FTC_LINEA,532,12) AS INT)) ID_44
		,SUM(CAST(substr(FTC_LINEA,544,12) AS INT)) ID_45
		,SUM(CAST(substr(FTC_LINEA,556,12) AS INT)) ID_46
		,SUM(CAST(substr(FTC_LINEA,568,12) AS INT)) ID_47
		,COUNT(1) ID_48
	FROM #DELTA_TABLE_NAME_001#
	WHERE substr(FTC_LINEA,1,2) = '02'
)
SELECT
	FTN_NO_LINEA
	,CASE WHEN substr(FTC_LINEA,1,2) = '09' then NULL else substr(FTC_LINEA,1,2) end Error_en_Tipo_de_registro_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,3,2) = '09' then NULL else substr(FTC_LINEA,3,2) end Error_en_Identificador_del_servicio_diferente_a_09
	,CASE WHEN substr(FTC_LINEA,5,2) = '71' then NULL else substr(FTC_LINEA,5,2) end Error_en_Identificador_de_operacion_diferente_a_71
	,CASE WHEN substr(FTC_LINEA,7,2) = '01' then NULL else substr(FTC_LINEA,7,2) end Error_en_Tipo_entidad_origen_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,9,3) = '534' then NULL else substr(FTC_LINEA,9,3) end Error_en_Clave_entidad_origen_diferente_a_534
	,CASE WHEN substr(FTC_LINEA,12,2) = '03' then NULL else substr(FTC_LINEA,12,2) end Error_en_Tipo_entidad_destino_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,14,3) = '001' then NULL else substr(FTC_LINEA,14,3) end Error_en_Clave_entidad_destino_diferente_a_Clave_de_la_AFORE_diferente_a_001
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') IS NOT NULL AND try_to_timestamp(substr(FTC_LINEA,17,8), 'yyyyMMdd') = RES_01.ID_08 then NULL ELSE substr(FTC_LINEA,17,8) END Error_en_Fecha_diferente_vs_fecha_del_encabezado
	,CASE WHEN substr(FTC_LINEA,25,3) = '000' then NULL else substr(FTC_LINEA,25,3) end Error_en_Consecutivo_diferente_vs_consecutivo_del_encabezado
	,CASE WHEN CAST(substr(FTC_LINEA, 28, 15) AS DOUBLE) = RES_02.ID_23 then NULL else substr(FTC_LINEA, 28, 15) end Error_en_Importe_SAR_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 43, 12) AS DOUBLE) = RES_02.ID_24 then NULL else substr(FTC_LINEA, 43, 12) end Error_en_Importe_Retiro_ISSSTE_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 55, 15) AS DOUBLE) = RES_02.ID_25 then NULL else substr(FTC_LINEA, 55, 15) end Error_en_Importe_CV_Aportacion_Patron_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 70, 15) AS DOUBLE) = RES_02.ID_26 then NULL else substr(FTC_LINEA, 70, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_01
	,CASE WHEN CAST(substr(FTC_LINEA, 85, 12) AS DOUBLE) = RES_02.ID_27 then NULL else substr(FTC_LINEA, 85, 12) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_02
	,CASE WHEN CAST(substr(FTC_LINEA, 112, 15) AS DOUBLE) = RES_02.ID_28 then NULL else substr(FTC_LINEA,112, 15) end Error_en_Importe_Total_de_Intereses_de_Pagos_Extemporaneos_de_Retiro_ISSSTE_SUMARIO_vs_DETALLE_02
	,CASE WHEN CAST(substr(FTC_LINEA, 127, 15) AS DOUBLE) = RES_02.ID_29 then NULL else substr(FTC_LINEA,127, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_03
	,CASE WHEN CAST(substr(FTC_LINEA, 142, 15) AS DOUBLE) = RES_02.ID_30 then NULL else substr(FTC_LINEA,142, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_04
	,CASE WHEN CAST(substr(FTC_LINEA, 157, 15) AS DOUBLE) = RES_02.ID_31 then NULL else substr(FTC_LINEA,157, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_05
	,CASE WHEN CAST(substr(FTC_LINEA, 172, 15) AS DOUBLE) = RES_02.ID_32 then NULL else substr(FTC_LINEA,172, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_06
	,CASE WHEN CAST(substr(FTC_LINEA, 187, 15) AS DOUBLE) = RES_02.ID_33 then NULL else substr(FTC_LINEA,187, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_07
	,CASE WHEN CAST(substr(FTC_LINEA, 202, 15) AS DOUBLE) = RES_02.ID_34 then NULL else substr(FTC_LINEA,202, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_08
	,CASE WHEN CAST(substr(FTC_LINEA, 217, 15) AS DOUBLE) = RES_02.ID_35 then NULL else substr(FTC_LINEA,217, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_09
	,CASE WHEN CAST(substr(FTC_LINEA, 232, 15) AS DOUBLE) = RES_02.ID_36 then NULL else substr(FTC_LINEA,232, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_10
	,CASE WHEN CAST(substr(FTC_LINEA, 247, 15) AS DOUBLE) = RES_02.ID_37 then NULL else substr(FTC_LINEA,247, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_11
	,CASE WHEN CAST(substr(FTC_LINEA, 262, 15) AS DOUBLE) = RES_02.ID_38 then NULL else substr(FTC_LINEA,262, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_12
	,CASE WHEN CAST(substr(FTC_LINEA, 277, 15) AS DOUBLE) = RES_02.ID_39 then NULL else substr(FTC_LINEA,277, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_13
	,CASE WHEN CAST(substr(FTC_LINEA, 292, 15) AS DOUBLE) = RES_02.ID_40 then NULL else substr(FTC_LINEA,292, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_14
	,CASE WHEN CAST(substr(FTC_LINEA, 307, 15) AS DOUBLE) = RES_02.ID_41 then NULL else substr(FTC_LINEA,307, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_15
	,CASE WHEN CAST(substr(FTC_LINEA, 322, 15) AS DOUBLE) = RES_02.ID_42 then NULL else substr(FTC_LINEA,322, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_16
	,CASE WHEN CAST(substr(FTC_LINEA, 337, 15) AS DOUBLE) = RES_02.ID_43 then NULL else substr(FTC_LINEA,337, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_17
	,CASE WHEN CAST(substr(FTC_LINEA, 352, 15) AS DOUBLE) = RES_02.ID_44 then NULL else substr(FTC_LINEA,352, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_18
	,CASE WHEN CAST(substr(FTC_LINEA, 367, 15) AS DOUBLE) = RES_02.ID_45 then NULL else substr(FTC_LINEA,367, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_19
	,CASE WHEN CAST(substr(FTC_LINEA, 382, 15) AS DOUBLE) = RES_02.ID_46 then NULL else substr(FTC_LINEA,382, 15) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_20
	,CASE WHEN CAST(substr(FTC_LINEA, 397, 9) AS DOUBLE) = RES_02.ID_47 then NULL else substr(FTC_LINEA, 397, 9) end Error_en_Importe_CV_Aportacion_Trabajador_SUMARIO_vs_DETALLE_02_21
FROM #DELTA_TABLE_NAME_001#
	JOIN RES_01
		ON 1 = 1
	JOIN RES_02
		ON 1 = 1
WHERE substr(FTC_LINEA,1,2) = '09'