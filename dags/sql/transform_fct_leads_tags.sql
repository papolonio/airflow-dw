INSERT INTO public.fct_leads_tags (id, tag_name, created_at)
SELECT
    id,
    tag_name,
    created_at
FROM
    public.stg_crm_leads_tags
ON CONFLICT (id, tag_name) DO NOTHING; -- Se a tag já está lá, não faz nada.