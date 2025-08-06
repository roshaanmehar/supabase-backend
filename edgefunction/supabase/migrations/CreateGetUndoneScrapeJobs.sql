CREATE OR REPLACE FUNCTION get_undone_scrape_jobs()
RETURNS TABLE (
  job_id uuid,
  profile_id uuid,
  scraper_engine text,
  created_at timestamptz,
  job_parts jsonb
) 
LANGUAGE plpgsql
AS $$
BEGIN
  RETURN QUERY
  SELECT 
    sj.id as job_id,
    sj.profile_id,
    sj.scraper_engine::text,
    sj.created_at,
    json_agg(
      json_build_object(
        'part_id', sjp.id,
        'postcode', sjp.postcode,
        'keyword', sjp.keyword,
        'city', sjp.city,
        'state', sjp.state,
        'country', sjp.country
      )
    )::jsonb as job_parts
  FROM scrape_jobs sj
  JOIN scrape_job_parts sjp ON sj.id = sjp.job_id
  WHERE sj.status = 'undone' AND sjp.status = 'undone'
  GROUP BY sj.id, sj.profile_id, sj.scraper_engine, sj.created_at
  ORDER BY sj.created_at ASC;
END;
$$;