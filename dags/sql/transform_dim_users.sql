INSERT INTO public.dim_users (id, name)
SELECT 
    id, 
    name
FROM public.stg_crm_users
ON CONFLICT (id) DO UPDATE SET
    name = EXCLUDED.name;