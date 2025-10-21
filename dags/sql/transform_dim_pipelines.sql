INSERT INTO public.dim_pipelines_status (pipeline_id, pipeline_name, status_id, status_name, status_sort, status_color)
SELECT 
    pipeline_id, 
    pipeline_name, 
    status_id, 
    status_name, 
    status_sort, 
    status_color
FROM public.stg_crm_pipelines_status
ON CONFLICT (status_id) DO UPDATE SET
    pipeline_name = EXCLUDED.pipeline_name,
    status_name = EXCLUDED.status_name,
    status_sort = EXCLUDED.status_sort,
    status_color = EXCLUDED.status_color;