from jobs.ats.sitemap import fetch_jobs, fetch_job_detail

companies = [
    ("Google",    {"url": "https://www.google.com/about/careers/applications/jobs/feed.xml"}),
    ("JnJ",       {"url": "https://www.careers.jnj.com/sitemap.xml"}),
    ("Nintendo",  {"url": "https://careers.nintendo.com/sitemap.xml"}),
    ("Elevance",  {"url": "https://careers.elevancehealth.com/sitemap-c8fbb5ed-en.xml"}),
    ("MyFlorida", {"url": "https://jobs.myflorida.com/sitemap.xml"}),
    ("Databricks",{"url": "https://www.databricks.com/careers-assets/sitemap/sitemap-0.xml"}),
    ("Airbnb",    {"url": "https://careers.airbnb.com/sitemap.xml"}),
]

for company, slug_info in companies:
    jobs = fetch_jobs(slug_info, company)
    print(f"\n{company}: {len(jobs)} jobs")
    if jobs:
        d = fetch_job_detail(jobs[0])
        print(f"  title:    {d['title']}")
        print(f"  location: {d['location']}")
        print(f"  date:     {d['posted_at']}")
        print(f"  desc:     {d['description'][:500]}")
        print(f"  url:     {d['job_url']}")