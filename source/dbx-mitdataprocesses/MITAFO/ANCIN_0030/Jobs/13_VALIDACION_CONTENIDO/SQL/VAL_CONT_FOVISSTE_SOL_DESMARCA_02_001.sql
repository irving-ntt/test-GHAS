SELECT
	FTN_NO_LINEA
  ,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,1,2))) = 0 THEN 'Registro_Vacio'
        WHEN substr(FTC_LINEA,1,2) = '02' THEN NULL 
        ELSE substr(FTC_LINEA,1,2) end Error_en_tipo_de_registro
	,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,3,2))) = 0 THEN 'Registro_Vacio'
        WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,3,2), '[0-9]*', '')) = 0 then NULL 
        ELSE substr(FTC_LINEA,3,2) end Error_en_Linea_de_credito_FOVISSSTE
	,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,5,1))) = 0 THEN 'Registro_Vacio'
        WHEN substr(FTC_LINEA,5,1) in ('1','2','3') then NULL 
        ELSE substr(FTC_LINEA,5,1) end Error_en_campo_tiptra
	-- Validación CURP (alfanumérico, obligatorio si NSS contiene espacios)
  ,CASE 
        --WHEN LENGTH(TRIM(substr(FTC_LINEA,6,18))) = 0 THEN 'Registro_Vacio'
        WHEN instr(substr(FTC_LINEA,32,11), ' ') > 0 -- NSS contiene espacios
             AND (substr(FTC_LINEA,6,18) IS NULL OR LENGTH(TRIM(substr(FTC_LINEA,6,18))) = 0) THEN 'CURP_obligatorio'
        --WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,6,18), '[A-Za-z0-9]*', '')) > 0 THEN substr(FTC_LINEA,6,18) -- contiene caracteres no alfanuméricos
        
        ELSE NULL
    END AS Error_en_campo_CURP
  
    -- Validación NSS (alfanumérico, obligatorio si CURP contiene espacios)
  ,CASE 
        --WHEN LENGTH(TRIM(substr(FTC_LINEA,32,11))) = 0 THEN 'Registro_Vacio'
        WHEN instr(substr(FTC_LINEA,6,18), ' ') > 0 -- CURP contiene espacios
             AND (substr(FTC_LINEA,32,11) IS NULL OR LENGTH(TRIM(substr(FTC_LINEA,32,11))) = 0) THEN 'NSS_obligatorio'
        --WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,32,11), '[A-Za-z0-9]*', '')) > 0 THEN substr(FTC_LINEA,32,11)
        
        ELSE NULL
    END AS Error_en_campo_NSS
   
    -- Validación RFC (alfanumérico)
  ,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,43,13))) = 0 THEN 'Registro_Vacio'
        WHEN TRIM(substr(FTC_LINEA,43,13)) RLIKE '^[0-9]+$' THEN 'RFC_erroneo_solo_contiene_numeros'
        WHEN TRIM(substr(FTC_LINEA,43,11)) RLIKE '^[0-9]+$' THEN 'RFC_erroneo_solo_contiene_numeros'
        WHEN TRIM(substr(FTC_LINEA,43,13)) RLIKE '^[A-Za-z]+$' THEN 'RFC_erroneo_solo_contiene_letras'
        WHEN TRIM(substr(FTC_LINEA,43,11)) RLIKE '^[A-Za-z]+$' THEN 'RFC_erroneo_solo_contiene_letras'
      ELSE NULL
    END AS Error_en_campo_RFC
  ,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,256,2))) = 0 THEN 'Registro_Vacio'
        WHEN substr(FTC_LINEA,256,2) in ('01','02') then NULL 
        else substr(FTC_LINEA,256,2) 
  end Error_en_campo_Tipo_de_Marca_de_Credito_diferente_de_01_o_02
  ,CASE 
        WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,256,2), '[0-9]*', '')) = 0 then NULL 
        else substr(FTC_LINEA,256,2) 
  end Error_en_campo_Tipo_de_Marca_de_Credito_diferente_de_numerico
  ,CASE 
        WHEN LENGTH(TRIM(substr(FTC_LINEA,258,2))) = 0 THEN 'Registro_Vacio'
        WHEN LENGTH(REGEXP_REPLACE(substr(FTC_LINEA,258,2), '[0-9]*', '')) = 0 then NULL 
        else substr(FTC_LINEA,258,2) 
  end Error_en_campo_Linea_de_Credito_diferente_de_numerico
FROM #DELTA_TABLE_NAME_001#
WHERE 1 = 1
	AND substr(FTC_LINEA,1,2) != '01'
	AND substr(FTC_LINEA,1,2) != '09'