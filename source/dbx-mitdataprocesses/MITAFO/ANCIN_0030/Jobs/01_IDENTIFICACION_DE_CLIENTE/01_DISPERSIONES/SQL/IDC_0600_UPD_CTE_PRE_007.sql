SELECT
  A.FTN_NUM_CTA_INVDUAL,
  B.FCN_ID_TIPO_SUBCTA,
  MAX(C.FCN_ID_GRUPO) FCN_ID_GRUPO,
  MAX(D.FCN_ID_SIEFORE) FCN_ID_SIEFORE
FROM
  #catalog_name#.#schema_name#.temp_df_a_#sr_id_archivo# A
  INNER JOIN #catalog_name#.#schema_name#.temp_df_b_#sr_id_archivo# B ON A.FTN_NSS_CURP = B.FTN_NSS_CURP
  INNER JOIN #catalog_name#.#schema_name#.temp_df_c_#sr_id_archivo# C ON B.FCN_ID_TIPO_SUBCTA = C.FCN_ID_TIPO_SUBCTA
  LEFT JOIN #catalog_name#.#schema_name#.temp_df_d_#sr_id_archivo# D ON A.FTN_NUM_CTA_INVDUAL = D.FTN_NUM_CTA_INVDUAL
  AND C.FCN_ID_GRUPO = D.FCN_ID_GRUPO
GROUP BY
  A.FTN_NUM_CTA_INVDUAL,
  B.FCN_ID_TIPO_SUBCTA