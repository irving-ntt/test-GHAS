BEGIN
    UPDATE PROCESOS.TTAFOTRAS_TRANS_FOVISSSTE 
    SET
        FTC_CODIGO_RES_OPER = NULL,
        FTC_DIAG_PROCESAR   = NULL,
        FTC_TIPO_ARCH       = '01',
        FTC_ESTATUS         = '1',
        FTN_MOTIVO_RECHAZO  = NULL
    WHERE FTC_FOLIO     = '201906201228140001'
      AND FTC_TIPO_ARCH = '09';

    COMMIT;  -- Aquí confirmas todos los cambios hechos en la transacción
END