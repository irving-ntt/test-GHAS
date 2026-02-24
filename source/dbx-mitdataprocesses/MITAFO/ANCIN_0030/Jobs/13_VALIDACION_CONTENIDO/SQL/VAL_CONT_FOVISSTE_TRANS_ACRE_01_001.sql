SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Error_en_campo_Identificador_de_Servicio_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,5,2) = '94' then NULL else substr(FTC_LINEA,5,2) end Error_en_campo_Identificador_de_Operacion_diferente_a_94
	,CASE WHEN substr(FTC_LINEA,7,2) = '03' then NULL else substr(FTC_LINEA,7,2) end Error_en_campo_Tipo_de_entidad_Origen_diferente_a_03
	,CASE WHEN substr(FTC_LINEA,9,3) = '001' then NULL else substr(FTC_LINEA,9,3) end Error_en_campo_Clave_de_entidad_Origen_diferente_a_001
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_campo_Tipo_de_Entidad_destino_diferente_a_01
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_campo_Clave_de_entidad_destino_diferente_a_534
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,21,8) end Error_en_campo_Fecha_de_Presentacion_diferente_a_fecha_valida_con_formato_igual_a_AAAAMMDD
	--,CASE WHEN SUBSTRING(RIGHT('#sr_path_arch#', 7), 1, 6) = substr(FTC_LINEA,22,6) then NULL else substr(FTC_LINEA,22,6) end Error_en_fecha_presentacion_vs_nombre_archivo
	--,CASE WHEN  SUBSTRING(RIGHT('#sr_path_arch#', 21), 1, 3) = substr(FTC_LINEA,28,3) then NULL else substr(FTC_LINEA,28,3) end Error_en_consecutivo_vs_nombre_archivo
	,CASE
        WHEN substring(FTC_LINEA, 22, 6) = 
				substring(
            element_at(
                filter(split('#sr_path_arch#', '\\.'), x -> x LIKE 'S______'),
                1
            ),
            2, 6
        )
        THEN NULL
        ELSE substring(FTC_LINEA, 22, 6)
    END AS Error_en_fecha_presentacion_vs_nombre_archivo
	
	
--,CASE
--        WHEN substring(FTC_LINEA, 28, 3) = 
--				substring(
--						element_at(
--								filter(split('#sr_path_arch#', '\\.'), x -> x LIKE 'C___'),
--								1
--						),
--						2, 3
--				)
--        THEN NULL
--        ELSE substring(FTC_LINEA, 28, 3)
--    END AS Error_en_consecutivo_vs_nombre_archivo
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'