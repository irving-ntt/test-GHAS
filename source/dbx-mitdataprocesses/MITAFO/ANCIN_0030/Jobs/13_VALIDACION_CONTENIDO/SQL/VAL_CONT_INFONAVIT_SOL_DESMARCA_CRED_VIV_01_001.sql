SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '01' then NULL else substr(FTC_LINEA,1,2) end Error_en_tipo_de_registro
 	,CASE WHEN substr(FTC_LINEA,3,2) = '02' then NULL else substr(FTC_LINEA,3,2) end Error_en_identificador_de_servicio
	,CASE WHEN substr(FTC_LINEA,5,2) = '30' then NULL else substr(FTC_LINEA,5,2) end Error_en_indicador_de_operacion
	,CASE WHEN substr(FTC_LINEA,7,2) = '04' then NULL else substr(FTC_LINEA,7,2) end Error_en_tipo_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,9,3) = '002' then NULL else substr(FTC_LINEA,9,3) end Error_en_clave_de_entidad_origen
	,CASE WHEN substr(FTC_LINEA,12,2) = '01' then NULL else substr(FTC_LINEA,12,2) end Error_en_tipo_de_entidad_destino
	,CASE WHEN substr(FTC_LINEA,14,3) = '534' then NULL else substr(FTC_LINEA,14,3) end Error_en_clave_de_entidad_destino
	,CASE WHEN try_to_timestamp(substr(FTC_LINEA,20,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,20,8) end Error_en_fecha_de_presentacion
  --,CASE WHEN substr('#sr_path_arch#',37,6) = substr(FTC_LINEA,22,6) then NULL else substr(FTC_LINEA,22,6) end Error_en_fecha_presentacion_vs_nombre_archivo
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
	--,CASE WHEN substr('#sr_path_arch#',51,3) = substr(FTC_LINEA,28,3) then NULL else substr(FTC_LINEA,28,3) end Error_en_consecutivo_vs_nombre_archivo
	,CASE
        WHEN substring(FTC_LINEA, 28, 3) = 
				substring(
						element_at(
								filter(split('#sr_path_arch#', '\\.'), x -> x LIKE 'C___'),
								1
						),
						2, 3
				)
        THEN NULL
        ELSE substring(FTC_LINEA, 28, 3)
    END AS Error_en_consecutivo_vs_nombre_archivo
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '02'
	AND substr(FTC_LINEA,1,2) != '09'