
INSERT INTO public.fct_events (
    id, 
    type, 
    entity_id, 
    created_at, 
    status_id, 
    pipeline_id
)
SELECT
    id::bigint,         
    type,
    entity_id::bigint,   
    created_at,          
    status_id::bigint,   
    pipeline_id::bigint  
FROM
    public.stg_crm_events
ON CONFLICT (id) DO NOTHING;