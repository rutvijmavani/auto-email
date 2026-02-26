from ai_personalizer import generate_job_based_intro
from job_fetcher import fetch_job_description

def get_template(stage, name, company , job_url):

    stage = stage or "initial"

    job_text = fetch_job_description(job_url)
    personalized_intro = generate_job_based_intro(company, job_text)


    if stage == "initial":
        if job_text and personalized_intro:
            return f"""
Hi {name},

I recently came across the Backend Engineer role at {company}:
{job_url}

{personalized_intro if personalized_intro else ""}

I would love the opportunity to discuss how I can contribute to your team.

I’ve attached my resume for your review.

Best,
Rutvij
"""

        return f"""
Hi {name},

I hope you're doing well.

I’m a Software Developer with experience building microservices-based systems using Python, Go, and JavaScript. 
At Celerant Technology, I worked on Kubernetes-based architectures, optimized database performance, 
and implemented CI/CD pipelines that improved deployment efficiency.

I’m particularly interested in backend and platform engineering opportunities at {company}. 
Given my experience with distributed systems, REST APIs, PostgreSQL, Docker, and cloud infrastructure, 
I would love to explore how I can contribute to your team.

I’ve attached my resume and would appreciate the opportunity to connect.

Best regards,  
Rutvij Mavani
"""

    elif stage == "followup1":
        if job_text and personalized_intro:
            return f"""
Hi {name},

I wanted to follow up regarding the Backend Engineer role at {company}:
{job_url}

Best,
Rutvij
"""
        return f"""
Hi {name},

I wanted to briefly follow up on my previous message regarding backend opportunities at {company}.

With hands-on experience in microservices architecture, Kubernetes deployments, CI/CD automation, 
and full-stack systems using React and Node.js, I’m confident I could add value to engineering teams 
focused on scalable backend systems.

Please let me know if there’s a good time to connect — I’d be happy to share more details about my experience.

Best regards,  
Rutvij
"""

    elif stage == "followup2":
        if job_text and personalized_intro:
            return f"""
Hi {name},

This is my final follow-up regarding the Backend Engineer role at {company}:
{job_url}

Regards,
Rutvij
"""
        return f"""
Hi {name},

Just checking in one last time regarding potential backend or software engineering roles at {company}.

Recently, I’ve also worked on projects involving event-driven workflows using Inngest, real-time data processing, 
and AI-powered automation features — experiences that strengthened my system design and distributed workflow knowledge.

If there’s someone else on your team I should reach out to, I’d greatly appreciate your guidance.

Thank you for your time,  
Rutvij
"""

    return None


def next_stage(stage):
    mapping = {
        "initial": "followup1",
        "followup1": "followup2",
        "followup2": "stopped"
    }

    return mapping.get(stage, "stopped")