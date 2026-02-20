select
  a.FTN_NUM_CTA_INVDUAL
from
  (
    select
      case
        when 3 = #p_ind# then FTN_NSS
        else (
          case
            when 2 = #p_ind# then FTC_CURP
            else '0'
          end
        )
      end FTN_NSS_CURP,
      FTN_NUM_CTA_INVDUAL
    from
      CIERREN.TTAFOGRAL_CTA_INVDUAL
  ) a
  INNER JOIN (
    SELECT
      distinct FTC_NSS_CURP
    FROM
      CIERREN_ETL.TTSISGRAL_ETL_DISPERSION_CTAS
    WHERE
      FTC_FOLIO = '#sr_folio#'
  ) b on a.FTN_NSS_CURP = b.FTC_NSS_CURP