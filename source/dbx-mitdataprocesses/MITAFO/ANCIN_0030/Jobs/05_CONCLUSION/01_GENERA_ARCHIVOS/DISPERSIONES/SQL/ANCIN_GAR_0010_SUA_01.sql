SELECT
        Y.FTC_FOLIO                                                                                                   ,
        Y.FTC_FOLIO_SUA                                                                                               ,
        Y.FTN_NUM_CTA_INVDUAL                                                                                         ,
        Y.FNN_ID_REFERENCIA                                                                                           ,
        Y.FTD_FEH_LIQUIDACION                                                                                         ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_MONRET, 'FM99999.90'),'.'),7,0)                AS DISP_MONRET                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACTRET, 'FM99999.90'),'.'),7,0)                AS DISP_ACTRET                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_MONCVE, 'FM99999.90'),'.'),7,0)                AS DISP_MONCVE                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACTCVE, 'FM99999.90'),'.'),7,0)                AS DISP_ACTCVE                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_MONVIV, 'FM99999.90'),'.'),7,0)                AS DISP_MONVIV                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_REMVIV, 'FM9999999999999.90'),'.'),15,0)       AS DISP_REMVIV                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_APINTREM, 'FM9999999999999.90'),'.'),15,0)     AS DISP_APINTREM                 ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_INTGENEXTVIV, 'FM9999999999999.90'),'.'),15,0) AS DISP_INTGENEXTVIV             ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_MONVOL, 'FM99999.90'),'.'),7,0)                AS DISP_MONVOL                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_APINTVIVEXT, 'FM9999999999999.90'),'.'),15,0)  AS DISP_APINTVIVEXT              ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACR, 'FM9999999999999.90'),'.'),15,0)          AS DISP_ACR                      ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_CUOSOC, 'FM99999.90'),'.'),7,0)                AS DISP_CUOSOC                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACTCSO, 'FM99999.90'),'.'),7,0)                AS DISP_ACTCSO                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_APOEST, 'FM99999.90'),'.'),7,0)                AS DISP_APOEST                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACTEST, 'FM99999.90'),'.'),7,0)                AS DISP_ACTEST                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_APOESP, 'FM99999.90'),'.'),7,0)                AS DISP_APOESP                   ,
        LPAD(REPLACE (TO_CHAR( Y.DISP_ACTESP, 'FM99999.90'),'.'),7,0)                AS DISP_ACTESP                   ,
        Z.FNN_NUMREG                                                                 AS FNN_NUMREG_SUA                ,
        Z.FNC_TIPREG                                                                 AS FNC_TIPREG_SUA                ,
        Z.FTN_NSS                                                                    AS FTN_NSS_SUA                   ,
        TO_CHAR(Z.FTN_CONS_REG_LOTE)                                                 AS FTN_CONS_REG_LOTE_SUA         ,
        Z.FTC_RFC                                                                    AS FTC_RFC_SUA                   ,
        Z.FTC_CURP                                                                   AS FTC_CURP_SUA                  ,
        Z.FTC_NOMBRE                                                                 AS FTC_NOMBRE_SUA                ,
        Z.FNC_PERIODO_PAGO_PATRON                                                    AS FNC_PERDO_PAGO_PATRN_SUA      ,
        Z.FND_FECHA_PAGO                                                             AS FND_FECHA_PAGO_SUA            ,
        Z.FND_FECHA_VALOR_IMSS_ACV_VIV                                               AS FND_FEC_VALOR_IMSS_SUA        ,
        Z.FNN_ULTIMO_SALARIO_INT_PER                                                 AS FNN_ULTIMO_SALARIO_INT_PER_SUA,
        Z.FNC_FOLIO_PAGO_SUA                                                         AS FNC_FOLIO_PAGO_SUA            ,
        Z.FNC_REG_PATRONAL_IMSS                                                      AS FNC_REG_PATRN_IMSS_SUA        ,
        Z.FNC_RFC_PATRON                                                             AS FNC_RFC_PATRON_SUA            ,
        TO_CHAR(Z.FNN_DIAS_COTZDOS_BIMESTRE)                                         AS FNN_DIAS_COTZDOS_BIM_SUA      ,
        TO_CHAR(Z.FNN_DIAS_INCAP_BIMESTRE)                                           AS FNN_DIAS_INCAP_BIM_SUA        ,
        TO_CHAR(Z.FNN_DIAS_AUSENT_BIMESTRE)                                          AS FNN_DIAS_AUSENT_BIM_SUA       ,
        TO_CHAR(Z.FNN_ID_VIV_GARANTIA)                                               AS FNN_ID_VIV_GARANTIA_SUA       ,
        Z.FNN_APLIC_INT_VIVIENDA                                                     AS FNN_APLIC_INT_VIV_SUA         ,
        Z.FNC_MOTIVO_LIBCION_APORT                                                   AS FNC_MOTIVO_LIBCION_APORT_SUA  ,
        Z.FND_FECHA_PAGO_CUOTA_GUB                                                   AS FND_FEC_PAGO_CUOTA_GUB_SUA    ,
        Z.FND_FECHA_VALOR_RCV                                                        AS FND_FEC_VALOR_RCV_SUA         ,
        Z.FND_FECTRA                                                                 AS FND_FECTRA_SUA                ,
        TO_CHAR(Z.FNN_SECLOT)                                                        AS FNN_SECLOT_SUA                ,
        Z.FNN_AFORE                                                                  AS FNN_AFORE_SUA                 ,
        Z.FNN_DEVPAG                                                                 AS FNN_DEVPAG_SUA                ,
        Z.FNN_CVE_ENT_RECEP_PAGO                                                     AS FNN_CVE_ENT_RECEP_PAGO_SUA    ,
        Z.FNN_NSS_ISSSTE                                                             AS FNN_NSS_ISSSTE_SUA            ,
        Z.FNC_DEPEND_CTRO_PAGO                                                       AS FNC_DEPEND_CTRO_PAGO_SUA      ,
        Z.FNN_CTRO_PAGO_SAR                                                          AS FNN_CTRO_PAGO_SAR_SUA         ,
        Z.FNN_CVE_RAMO                                                               AS FNN_CVE_RAMO_SUA              ,
        Z.FNC_CVE_PAGADURIA                                                          AS FNC_CVE_PAGADURIA_SUA         ,
        Z.FNN_SUELDO_BASICO_COTIZ_RCV                                                AS FNN_SUELDO_BAS_COTIZ_RCV_SUA  ,
        Z.FNC_ID_TRABAJADOR_BONO                                                     AS FNC_ID_TRAB_BONO_SUA          ,
        Z.FND_FEH_CRE                                                                AS FND_FEH_CRE_SUA               ,
        Z.FNC_USU_CRE                                                                AS FNC_USU_CRE_SUA
FROM
        (
                SELECT
                        FTC_FOLIO                                  ,
                        FTC_FOLIO_SUA                              ,
                        FTN_NUM_CTA_INVDUAL                        ,
                        FCN_ID_TIPO_MOV                            ,
                        FNN_ID_REFERENCIA                          ,
                        FTD_FEH_LIQUIDACION                        ,
                        SUM(DISP_MONRET)       AS DISP_MONRET      ,
                        SUM(DISP_ACTRET)       AS DISP_ACTRET      ,
                        SUM(DISP_MONCVE)       AS DISP_MONCVE      ,
                        SUM(DISP_ACTCVE)       AS DISP_ACTCVE      ,
                        SUM(DISP_MONVIV)       AS DISP_MONVIV      ,
                        SUM(DISP_REMVIV)       AS DISP_REMVIV      ,
                        SUM(DISP_APINTREM)     AS DISP_APINTREM    ,
                        SUM(DISP_INTGENEXTVIV) AS DISP_INTGENEXTVIV,
                        SUM(DISP_MONVOL)       AS DISP_MONVOL      ,
                        SUM(DISP_APINTVIVEXT)  AS DISP_APINTVIVEXT ,
                        SUM(DISP_ACR)          AS DISP_ACR         ,
                        SUM(DISP_CUOSOC)       AS DISP_CUOSOC      ,
                        SUM(DISP_ACTCSO)       AS DISP_ACTCSO      ,
                        SUM(DISP_APOEST)       AS DISP_APOEST      ,
                        SUM(DISP_ACTEST)       AS DISP_ACTEST      ,
                        SUM(DISP_APOESP)       AS DISP_APOESP      ,
                        SUM(DISP_ACTESP)       AS DISP_ACTESP
                FROM
                        (
                                SELECT
                                        FTC_FOLIO,
                                        CASE
                                        WHEN
                                                (FTC_FOLIO_REL IS NULL )
                                        THEN
                                                FTC_FOLIO
                                        ELSE
                                                FTC_FOLIO_REL
                                        END FTC_FOLIO_SUA  ,
                                        FTN_NUM_CTA_INVDUAL,
                                        FCN_ID_TIPO_MOV    ,
                                        FNN_ID_REFERENCIA  ,
                                        FTF_MONTO_PESOS    ,
                                        FTF_MONTO_ACCIONES ,
                                        FTD_FEH_LIQUIDACION,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 221 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_MONRET,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 222 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_ACTRET,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 223 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_MONCVE,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 224 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_ACTCVE,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 225 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_MONVIV,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 226 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_REMVIV,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 226 )
                                        THEN
                                                FTF_MONTO_ACCIONES
                                        ELSE
                                                0
                                        END DISP_APINTREM,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 227 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_INTGENEXTVIV,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 228 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_MONVOL,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 227 )
                                        THEN
                                                FTF_MONTO_ACCIONES
                                        ELSE
                                                0
                                        END DISP_APINTVIVEXT,
                                        CASE
                                        WHEN
                                                (FCN_ID_CONCEPTO_MOV = 229 )
                                        THEN
                                                FTF_MONTO_PESOS
                                        ELSE
                                                0
                                        END DISP_ACR      ,
                                        0   AS DISP_CUOSOC,
                                        0   AS DISP_ACTCSO,
                                        0   AS DISP_APOEST,
                                        0   AS DISP_ACTEST,
                                        0   AS DISP_APOESP,
                                        0   AS DISP_ACTESP
                                FROM
                                        CIERREN_ETL.TTSISGRAL_ETL_MOVIMIENTOS
                                WHERE
                                        FTC_FOLIO='#SR_FOLIO#' ) A 
                GROUP BY
                        FTC_FOLIO          ,
                        FTC_FOLIO_SUA      ,
                        FTN_NUM_CTA_INVDUAL,
                        FCN_ID_TIPO_MOV    ,
                        FNN_ID_REFERENCIA  ,
                        FTD_FEH_LIQUIDACION )Y
LEFT JOIN
        CIERREN.TNAFORECA_SUA Z
ON
        Z.FNN_ID_REFERENCIA  =Y.FNN_ID_REFERENCIA
AND     Z.FTC_FOLIO          =Y.FTC_FOLIO_SUA
AND     Z.FTN_NUM_CTA_INVDUAL=Y.FTN_NUM_CTA_INVDUAL