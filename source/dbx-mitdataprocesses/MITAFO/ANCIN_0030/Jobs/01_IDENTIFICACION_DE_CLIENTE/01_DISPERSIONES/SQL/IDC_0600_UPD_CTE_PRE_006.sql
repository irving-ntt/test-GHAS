SELECT
  O.FCN_ID_SIEFORE,
  O.FCN_ID_GRUPO,
  O.FTN_NUM_CTA_INVDUAL
FROM
  #catalog_name#.#schema_name#.temp_df_d1_#sr_id_archivo# O
  INNER JOIN #catalog_name#.#schema_name#.temp_df_d2_#sr_id_archivo# a ON o.FTN_NUM_CTA_INVDUAL = a.FTN_NUM_CTA_INVDUAL