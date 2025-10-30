WITH latest_cf_staging AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id, field_name ORDER BY created_at DESC) as rn
    FROM
        public.stg_crm_leads_custom_fields
)
INSERT INTO public.fct_leads_custom_fields (id, field_name, value, created_at)
SELECT
    id,
    field_name,
    value,
    created_at
FROM
    latest_cf_staging
WHERE
    rn = 1
ON CONFLICT (id, field_name) DO UPDATE SET
    value = EXCLUDED.value,
    created_at = EXCLUDED.created_at;