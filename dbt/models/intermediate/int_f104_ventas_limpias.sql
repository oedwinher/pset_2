{{ config(materialized='table') }}

{#-
  SILVER - int_f104_ventas_limpias
  Tratamiento de nulos (cada decision esta justificada):
   1. codigo_industria NULL  -> es una fila "sin clasificar CIIU" (1 por mes y corte): son
      contribuyentes cuyo RUC no tiene actividad CIIU asignada. NO se imputa a ninguna rama
      (hacerlo sesgaria las 46 series); se conserva con el centinela '000' y la bandera
      es_sin_clasificar para poder conciliar el total nacional. No pasa a Gold.
   2. ventas_nominales_usd = 0 con declarantes > 0 -> NO se imputa ni se elimina: el cero es un
      dato observable (rama sin ventas gravadas declaradas aun); imputar fabricaria ventas.
      Se marca con flag_ventas_cero_con_declarantes.
   3. Cortes temprano ausentes (rama-mes sin declaraciones a los 15 dias) -> ausencia
      estructural, no se rellena; la fila simplemente no existe y el ratio queda NULL en Spark.
   4. Filas sin fecha_mes, tipo_corte o ventas validas (parseo fallido en Bronze) -> se descartan
      de Silver porque no se pueden ubicar en el grano; el conteo se audita con tests.

  Deflactor: indice IPC SIMULADO (ver vars en dbt_project.yml); sustituible por el IPC real de INEC.
  Indicadores de coherencia economica: ver columnas flag_*.
-#}

with bronze as (

    select * from {{ ref('stg_f104_ventas') }}
    where fecha_mes is not null
      and tipo_corte is not null
      and ventas_nominales_usd is not null

),

deflactor as (

    select
        fecha_mes,
        100 * pow(
            1 + {{ var('ipc_tasa_mensual') }},
            datediff('month', to_date('{{ var('ipc_base_fecha') }}'), fecha_mes)
        ) as indice_ipc_sim
    from (select distinct fecha_mes from bronze)

),

enriquecido as (

    select
        b.anio_fiscal,
        b.mes_fiscal,
        coalesce(b.codigo_industria, '000')                           as codigo_industria,
        coalesce(b.nombre_industria, 'SIN CLASIFICAR (CIIU no reportado)') as nombre_industria,
        b.fecha_mes,
        b.tipo_corte,
        b.ventas_nominales_usd,
        b.num_declarantes,
        d.indice_ipc_sim,
        round(b.ventas_nominales_usd / (d.indice_ipc_sim / 100), 2)   as ventas_reales_usd,
        round(b.ventas_nominales_usd / nullif(b.num_declarantes, 0), 2) as ventas_por_declarante_usd,
        max(case when b.tipo_corte = 'definitivo' then b.ventas_nominales_usd end) over (
            partition by coalesce(b.codigo_industria, '000'), b.fecha_mes
        )                                                             as ventas_definitivo_mes_usd,
        b.codigo_industria is null                                    as es_sin_clasificar,
        b._loaded_at
    from bronze b
    inner join deflactor d on d.fecha_mes = b.fecha_mes

)

select
    anio_fiscal,
    mes_fiscal,
    codigo_industria,
    nombre_industria,
    fecha_mes,
    tipo_corte,
    ventas_nominales_usd,
    ventas_reales_usd,
    num_declarantes,
    indice_ipc_sim,
    ventas_por_declarante_usd,
    -- Cobertura del corte temprano: ventas a 15 dias / ventas definitivas del mismo mes y rama
    case when tipo_corte = 'temprano_15d'
         then round(ventas_nominales_usd / nullif(ventas_definitivo_mes_usd, 0), 4)
    end                                                               as ratio_temprano_definitivo,
    es_sin_clasificar,
    -- Coherencia economica
    (ventas_nominales_usd = 0 and coalesce(num_declarantes, 0) > 0)   as flag_ventas_cero_con_declarantes,
    (ventas_nominales_usd > 0 and coalesce(num_declarantes, 0) = 0)   as flag_ventas_sin_declarantes,
    (tipo_corte = 'temprano_15d'
        and ventas_definitivo_mes_usd is not null
        and ventas_nominales_usd > ventas_definitivo_mes_usd)         as flag_temprano_mayor_definitivo,
    (anio_fiscal <> year(fecha_mes) or mes_fiscal <> month(fecha_mes)) as flag_periodo_inconsistente,
    _loaded_at
from enriquecido
