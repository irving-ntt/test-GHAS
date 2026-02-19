SELECT
	FTN_NO_LINEA
  ,CASE WHEN substr(FTC_LINEA,1,2) = '02' then NULL else substr(FTC_LINEA,1,2) end Error_en_campo_Tipo_de_Registro_diferente_a_02
	,CASE WHEN substr(FTC_LINEA,202,2) in ('01','02')  then NULL else substr(FTC_LINEA,202,2) end Error_en_campo_tipo_operacion_distinto_a_los_valores_del_catalogo
	,CASE WHEN substr(FTC_LINEA,204,3) in ('001','002')  then NULL else substr(FTC_LINEA,204,3) end Error_en_campo_instituto_origen_distinto_a_los_valores_del_catalogo
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,207,10))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(REPLACE(substr(FTC_LINEA,207,10),' ','0'), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,207,10) end Error_en_campo_Identificador_de_credito_INFONAVIT_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,217,12))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,217,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,217,12) end Error_en_campo_Monto_en_Pesos_Infonavit97_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,229,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,229,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,229,15) end Error_en_campo_Valore_AIV_Infonavit97_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,244,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,244,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,244,8) end Error_en_campo_Fecha_de_Valor_AIVS_Infonavit_97_formato_de_fecha_invalida_formato_igual_a_AAAAMMDD 
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,225,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,252,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,252,15) end Error_en_campo_Total_AIV_de_Infonavit97_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,267,10))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(REPLACE(substr(FTC_LINEA,267,10),' ','0'), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,267,10) end Error_en_campo_Identificador_de_credito_FOVISSSTE_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,277,12))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,277,12), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,277,12) end Error_en_campo_Monto_en_Pesos_FOVISSTE_2008_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,289,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,289,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,289,15) end Error_en_campo_Valore_AIV_FOVISSSTE_2008_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,304,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,304,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,304,8) end Error_en_campo_Fecha_de_Valor_AIVS_FOVISSSTE_2008_formato_de_fecha_invalida_formato_igual_a_AAAAMMDD
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,312,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,312,15), '[0-9]*', '')) = 0 then NULL else substr(FTC_LINEA,312,15) end Error_en_campo_Total_AIV_de_FOVISSSTE_2008_diferente_a_dato_numerico
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,327,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,327,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,327,8) end Error_en_campo_Fecha_valor_del_credito_formato_de_fecha_invalida_formato_igual_a_AAAAMMDD
	,CASE 
				--WHEN LENGTH(TRIM(substr(FTC_LINEA,335,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,335,8), 'yyyyMMdd') IS NOT NULL then NULL else substr(FTC_LINEA,335,8) end Error_en_campo_Fecha_valor_de_la_transferencia_formato_de_fecha_invalida_formato_igual_a_AAAAMMDD

	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,351,12))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,351,12), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,351,12) 
		end Error_en_campo_montoPesosINFONAVIT92
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,363,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,363,15), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,363,15) 
		end Error_en_campo_valorAIVINFONAVIT92
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,378,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,378,8), 'yyyyMMdd') IS NOT NULL then NULL 
				else substr(FTC_LINEA,378,8) 
		end Error_en_campo_fechaValorAIVINFONAVIT92 ------------fecha ??? 00010101
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,386,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,386,15), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,386,15) 
		end Error_en_campo_totalAIVViviendaINFONAVIT92
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,401,12))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,401,12), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,401,12) 
		end Error_en_campo_montoPesosFOVISSSTE92
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,413,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,413,15), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,413,15) 
		end Error_en_campo_valorAIVFOVISSSTE92
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,428,8))) = 0 THEN 'Registro_Vacio'
				WHEN try_to_timestamp(substr(FTC_LINEA,428,8), 'yyyyMMdd') IS NOT NULL then NULL 
				else substr(FTC_LINEA,428,8) 
		end Error_en_campo_fechaValorAIVFOVISSSTE92 ------------fecha ??? 00010101
	,CASE 
				WHEN LENGTH(TRIM(substr(FTC_LINEA,436,15))) = 0 THEN 'Registro_Vacio'
				WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,436,15), '[0-9]*', '')) = 0 then NULL 
				else substr(FTC_LINEA,436,15) 
		end Error_en_campo_totalAIVViviendaFOVISSSTE92


FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'