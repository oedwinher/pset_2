-- Regla de negocio: el modelo se define sobre las 46 ramas CIIU (CNT). Falla si dim_industria no tiene exactamente 46.
select count(*) as n_ramas
from {{ ref('dim_industria') }}
having count(*) <> 46
