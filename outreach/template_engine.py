from jobs.job_fetcher import fetch_job_description
from outreach.ai_full_personalizer import generate_all_content


def get_template(stage, name, company, job_url):

    stage = stage or "initial"

    job_text = None
    personalized_intro = None
    follow_up_body = None
    job_title = "Software Engineer"

    if job_url:
        job_text = fetch_job_description(job_url)

    if job_text:
        for line in job_text.split("\n"):
            if "Job Title:" in line:
                job_title = line.replace("Job Title:", "").strip()
                break

    if job_text:
        ai_content = generate_all_content(company, job_title, job_text)
        personalized_intro = ai_content.get("intro")

        if stage == "followup1":
            follow_up_body = ai_content.get("followup1")
        elif stage == "followup2":
            follow_up_body = ai_content.get("followup2")

        subject = ai_content.get(
            f"subject_{stage}",
            f"{company} – Software Engineer Interest"
        )
    else:
        subject = f"{company} – Software Engineer Interest"

    if stage == "initial":
        if job_text and personalized_intro:
            body = f"""
Hi {name},

I recently came across the Backend Engineer role at {company}:
{job_url}

{personalized_intro if personalized_intro else ""}

I would love the opportunity to discuss how I can contribute to your team.

I've attached my resume for your review.

Best,
Rutvij
"""
        else:
            body = f"""
Hi {name},

I hope you're doing well.

I'm a Software Developer with experience building microservices-based systems using Python, Go, and JavaScript. 
At Celerant Technology, I worked on Kubernetes-based architectures, optimized database performance, 
and implemented CI/CD pipelines that improved deployment efficiency.

I'm particularly interested in backend and platform engineering opportunities at {company}. 
Given my experience with distributed systems, REST APIs, PostgreSQL, Docker, and cloud infrastructure, 
I would love to explore how I can contribute to your team.

I've attached my resume and would appreciate the opportunity to connect.

Best regards,  
Rutvij Mavani
"""
        return body, subject

    elif stage == "followup1":
        if job_text and personalized_intro and follow_up_body:
            body = f"""
Hi {name},

{follow_up_body if follow_up_body else ""}

Please let me know if there's a good time to connect — I'd be happy to share more details about my experience.

Best,
Rutvij
"""
        else:
            body = f"""
Hi {name},

I wanted to briefly follow up on my previous message regarding backend opportunities at {company}.

With hands-on experience in microservices architecture, Kubernetes deployments, CI/CD automation, 
and full-stack systems using React and Node.js, I'm confident I could add value to engineering teams 
focused on scalable backend systems.

Please let me know if there's a good time to connect — I'd be happy to share more details about my experience.

Best regards,  
Rutvij
"""
        return body, subject

    elif stage == "followup2":
        if job_text and personalized_intro and follow_up_body:
            body = f"""
Hi {name},

{follow_up_body if follow_up_body else ""}

Regards,
Rutvij
"""
        else:
            body = f"""
Hi {name},

Just checking in one last time regarding potential backend or software engineering roles at {company}.

Recently, I've also worked on projects involving event-driven workflows using Inngest, real-time data processing, 
and AI-powered automation features — experiences that strengthened my system design and distributed workflow knowledge.

If there's someone else on your team I should reach out to, I'd greatly appreciate your guidance.

Thank you for your time,  
Rutvij
"""
        return body, subject

    return None