{{ config(materialized='table') }}

{#-
  BRONZE - stg_f104_ventas
  * Unico modelo que toca RAW (source()).
  * Tipado explicito (RAW es todo VARCHAR) con TRY_*: un valor invalido queda NULL y se
    detecta en Silver/tests, en lugar de romper la carga.
  * Renombrado estandar a snake_case de negocio.
  * Deduplicacion: ROW_NUMBER() sobre la clave de negocio (mes_fiscal_id, codigo_industria,
    tipo_corte), conservando el registro mas reciente (_loaded_at DESC) = ultima vintage.
-#}

with fuente as (

    select * from {{ source('raw', 'raw_f104_ventas') }}

),

tipado as (

    select
        try_to_number(trim(anio_fiscal))                                   ::number(4, 0)  as anio_fiscal,
        try_to_number(trim(mes_fiscal))                                    ::number(2, 0)  as mes_fiscal,
        lpad(nullif(trim(codigo_industria_cnt_46), ''), 3, '0')            ::varchar(3)    as codigo_industria,
        nullif(regexp_replace(trim(descripcion_industria_cnt_46), '\\s+', ' '), '')
                                                                           ::varchar(300)  as nombre_industria,
        round(try_to_double(trim(ventas_totales)), 2)                      ::number(20, 2) as ventas_nominales_usd,
        try_to_number(trim(num_declarantes))                               ::number(12, 0) as num_declarantes,
        lower(trim(tipo_corte))                                            ::varchar(20)   as tipo_corte,
        try_to_date(trim(mes_fiscal_id))                                                   as fecha_mes,
        _batch_id,
        _loaded_at
    from fuente

),

deduplicado as (

    select
        *,
        row_number() over (
            partition by fecha_mes, codigo_industria, tipo_corte
            order by _loaded_at desc, _batch_id desc
        ) as rn
    from tipado

)

select
    anio_fiscal,
    mes_fiscal,
    codigo_industria,
    nombre_industria,
    ventas_nominales_usd,
    num_declarantes,
    tipo_corte,
    fecha_mes,
    _batch_id,
    _loaded_at
from deduplicado
where rn = 1
