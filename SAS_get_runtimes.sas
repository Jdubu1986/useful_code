proc sql noprint;
    /* Create a table named exa_update_times */
    create table exa_update_times as
    select
        /* Extract the date part from STTIME and format it */
        datepart(STTIME) as date format date10.,
        /* Select the flow name */
        flow_name,
        /* Select the job name */
        jobname,
        /* Find the minimum time part of STTIME and format it */
        min(timepart(STTIME)) as STTIME format time10.,
        /* Find the maximum time part of ENDTIME and format it */
        max(timepart(ENDTIME)) as ENDTIME format time10.,
        /* Calculate the duration in minutes (ENDTIME - STTIME) */
        (calculated ENDTIME - calculated STTIME)/60 as duration
        /*
        This code references the JOB_STATUS table from the specified schema.
        Ensure that the table name "JOB_STATUS" matches your database schema.
        If your table is named differently, update the reference accordingly.
        */
        from schema.JOB_STATUS 
        /* Filter the data based on date, flow name, and job names */
        (where =(today()>= datepart(STTIME)> today()-30 
                and flow_name = 'your_flow_name' /* Filter for a specific flow name. Replace 'your_flow_name' with the actual flow name. */
                and jobname in ('Job_name_1', 'job_name_2', 'job_name_3') /* Filter for specific job names. Replace with actual job names. */
               ))
        /* Group the results by flow name, date, and job name */
        group by flow_name, date, jobname
        /* Order the results by date, job name (descending), and start time */
        order by date, jobname DESCENDING, STTIME
    ;
quit;