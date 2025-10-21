-- 1. Identifica os leads mais recentes na staging
WITH latest_leads_staging AS (
    SELECT
        *,
        -- Particiona por 'id' e ordena por 'updated_at' (mais recente primeiro)
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC, created_at DESC) as rn
    FROM
        public.stg_crm_leads
)
-- 2. Insere/Atualiza na tabela final (fct_leads)
INSERT INTO public.fct_leads (
    id, name, price, responsible_user_id, status_id, pipeline_id, created_by, updated_by,
    created_at, updated_at, closed_at, companies_ids, loss_reason_ids, contacts_ids
)
SELECT
    id, name, price, responsible_user_id, status_id, pipeline_id, created_by, updated_by,
    created_at, updated_at, closed_at, companies_ids, loss_reason_ids, contacts_ids
FROM
    latest_leads_staging
WHERE
    rn = 1 -- Pega apenas a versão mais recente de cada lead da staging
-- 3. Lógica de UPSERT
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    responsible_user_id = EXCLUDED.responsible_user_id,
    status_id = EXCLUDED.status_id,
    pipeline_id = EXCLUDED.pipeline_id,
    updated_by = EXCLUDED.updated_by,
    updated_at = EXCLUDED.updated_at,
    closed_at = EXCLUDED.closed_at,
    companies_ids = EXCLUDED.companies_ids,
    loss_reason_ids = EXCLUDED.loss_reason_ids,
    contacts_ids = EXCLUDED.contacts_ids
-- Opcional: Só atualiza se o registro da staging for realmente mais novo
WHERE public.fct_leads.updated_at IS NULL OR public.fct_leads.updated_at < EXCLUDED.updated_at;