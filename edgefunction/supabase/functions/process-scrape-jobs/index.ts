import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { Deno } from "https://deno.land/std@0.168.0/runtime.ts"; // Declaring Deno variable

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
};

interface JobPart {
  part_id: string;
  postcode: string | null;
  keyword: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
}

interface ScrapeJob {
  job_id: string;
  profile_id: string;
  scraper_engine: string;
  created_at: string;
  job_parts: JobPart[];
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  try {
    // Get environment variables
    const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
    const serviceRoleKey = Deno.env.get("SERVICE_ROLE_KEY")!;
    const flaskServerUrl = Deno.env.get("REDIS_URL")!; // This is actually your Flask server URL now

    console.log("Environment check:", {
      hasSupabaseUrl: !!supabaseUrl,
      hasServiceKey: !!serviceRoleKey,
      hasFlaskUrl: !!flaskServerUrl,
      flaskUrl: flaskServerUrl, // Remove this in production
    });

    if (!serviceRoleKey) {
      throw new Error("SERVICE_ROLE_KEY environment variable is required");
    }

    if (!flaskServerUrl) {
      throw new Error("REDIS_URL environment variable is required");
    }

    const supabase = createClient(supabaseUrl, serviceRoleKey);

    // Query for undone jobs
    const { data: jobs, error: queryError } = await supabase.rpc(
      "get_undone_scrape_jobs"
    );

    if (queryError) {
      console.error("Database query error:", queryError);
      throw queryError;
    }

    if (!jobs || jobs.length === 0) {
      return new Response(
        JSON.stringify({
          message: "No undone jobs found",
          timestamp: new Date().toISOString(),
        }),
        {
          headers: { ...corsHeaders, "Content-Type": "application/json" },
          status: 200,
        }
      );
    }

    console.log(`Found ${jobs.length} undone jobs to process`);

    const processedJobs = [];
    const failedJobs = [];

    for (const job of jobs) {
      try {
        console.log(`Processing job: ${job.job_id}`);

        // Prepare job data in the exact format your Flask API expects
        const jobData = {
          job_id: job.job_id,
          profile_id: job.profile_id,
          scraper_engine: job.scraper_engine,
          created_at: job.created_at,
          job_parts: job.job_parts,
        };

        console.log(`Sending to Flask API: ${flaskServerUrl}/api/jobs/submit`);

        // Send to Flask API
        const flaskResponse = await fetch(`${flaskServerUrl}/api/jobs/submit`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(jobData),
        });

        if (!flaskResponse.ok) {
          const errorText = await flaskResponse.text();
          console.error(`Flask API error for job ${job.job_id}:`, errorText);
          failedJobs.push({
            job_id: job.job_id,
            error: `Flask API error (${flaskResponse.status}): ${errorText}`,
          });
          continue;
        }

        const flaskResult = await flaskResponse.json();
        console.log(`Flask API response for job ${job.job_id}:`, flaskResult);

        // Check if Flask API returned success
        if (!flaskResult.success) {
          console.error(
            `Flask API returned failure for job ${job.job_id}:`,
            flaskResult.error
          );
          failedJobs.push({
            job_id: job.job_id,
            error: `Flask API failure: ${flaskResult.error}`,
          });
          continue;
        }

        // Update job status to 'ongoing'
        const { error: updateJobError } = await supabase
          .from("scrape_jobs")
          .update({ status: "ongoing" })
          .eq("id", job.job_id);

        if (updateJobError) {
          console.error(`Failed to update job ${job.job_id}:`, updateJobError);
          failedJobs.push({
            job_id: job.job_id,
            error: `DB update error: ${updateJobError.message}`,
          });
          continue;
        }

        // Update job parts status to 'ongoing'
        const { error: updatePartsError } = await supabase
          .from("scrape_job_parts")
          .update({ status: "ongoing" })
          .eq("job_id", job.job_id);

        if (updatePartsError) {
          console.error(
            `Failed to update job parts for ${job.job_id}:`,
            updatePartsError
          );
          failedJobs.push({
            job_id: job.job_id,
            error: `DB parts update error: ${updatePartsError.message}`,
          });
          continue;
        }

        processedJobs.push({
          job_id: job.job_id,
          flask_response: flaskResult,
        });
        console.log(`Successfully processed job: ${job.job_id}`);
      } catch (jobError) {
        console.error(`Error processing job ${job.job_id}:`, jobError);
        failedJobs.push({ job_id: job.job_id, error: jobError.message });
      }
    }

    return new Response(
      JSON.stringify({
        success: true,
        message: `Processed ${processedJobs.length} jobs successfully, ${failedJobs.length} failed`,
        processed_jobs: processedJobs,
        failed_jobs: failedJobs,
        total_found: jobs.length,
        timestamp: new Date().toISOString(),
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 200,
      }
    );
  } catch (error) {
    console.error("Edge function error:", error);
    return new Response(
      JSON.stringify({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString(),
      }),
      {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
        status: 500,
      }
    );
  }
});
console.log("This code is a Supabase edge function that we can deply on Supabase and that runs whenever a record is inserted in the scrape_jobs")