SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Tipo_Registro
	--,Contador_de_Servicio
	,CASE WHEN substr(FTC_LINEA,13,3) = '534' then NULL else substr(FTC_LINEA,13,3) end Clave_de_entidad_emisora_del_movimiento
	--,CASE WHEN substr(FTC_LINEA,16,35) = REPEAT(' ', 35) then NULL else substr(FTC_LINEA,16,35) end Filler_01
	--,CASE WHEN substr(FTC_LINEA,51,11) = REPEAT(' ', 11) then NULL else substr(FTC_LINEA,51,11) end Filler_02 --,NSS_del_trabajador
	--,CASE WHEN substr(FTC_LINEA,62,108) = REPEAT(' ', 108) then NULL else substr(FTC_LINEA,62,108) end Filler_03
	,CASE WHEN substr(FTC_LINEA,170,2) in ('01','02','  ') then NULL else substr(FTC_LINEA,170,2) end Codigo_Resul_Ope
	,CASE WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,172,3), '[0-9 ]*', '')) = 0 then NULL else substr(FTC_LINEA,172,3) end Diagnostico_Proceso
	--,CASE WHEN substr(FTC_LINEA,175,556) = REPEAT(' ', 556) then NULL else substr(FTC_LINEA,175,556) end Filler_04
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'

	   
