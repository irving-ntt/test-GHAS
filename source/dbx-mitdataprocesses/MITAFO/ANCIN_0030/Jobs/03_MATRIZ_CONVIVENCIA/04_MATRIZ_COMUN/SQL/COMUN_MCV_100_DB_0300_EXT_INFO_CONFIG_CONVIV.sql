-- optimizada por GAIA
-- Extrae los registros de la configuracion de convivencia 
SELECT 
       CC.FFB_CONVIVENCIA,
       CC.FCN_ID_SUBPROCESO FTN_ID_SUBPROCESO,
       CC.FCN_ID_SUBPROCESO_CON,
       CC.FCN_ID_PROCESO_CON,
       1 AS B_CONVIV
FROM   CIERREN.TFAFOGRAL_CONFIG_CONVIV CC
WHERE  CC.FFB_VIGENTE = '1'