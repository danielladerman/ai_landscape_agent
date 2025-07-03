import openai
from config.config import settings
import json

def generate_personalized_email(business_name, titles, icebreaker, pains, solutions, evidence):
    """
    Generates a hyper-personalized cold email using OpenAI's GPT-4.

    Args:
        business_name (str): The name of the business.
        titles (str): A string of job titles found (e.g., "Owner, CEO").
        icebreaker (str): A genuine compliment about the business.
        pains (str): A JSON string of identified pain points.
        solutions (str): A JSON string of proposed solutions.
        evidence (str): A JSON string of the evidence for the pain points.

    Returns:
        dict: A dictionary containing the 'subject' and 'body' of the email,
              or None if an error occurs.
    """
    openai.api_key = settings.OPENAI_API_KEY

    # --- Deconstruct the analysis ---
    pain_point = json.loads(pains)[0] if pains and pains != '[]' else "attracting high-value clients"
    solution = json.loads(solutions)[0] if solutions and solutions != '[]' else "a bespoke social media strategy"
    specific_evidence = json.loads(evidence)[0] if evidence and evidence != '[]' else "Based on your impressive portfolio"

    # --- Define the new, unified persona and strategy ---
    persona_title = "Brand & Content Strategist"
    role_description = f"""
    You are a {persona_title} specializing in building digital assets for premium home service businesses.
    You understand that high-net-worth clients don't just 'search for a landscaper'; they look for an authoritative, trusted brand with a stunning portfolio.
    Your goal is to communicate the value of building an aspirational brand on Instagram, not just 'getting more leads.'
    """

    # --- Craft the new, focused prompt ---
    prompt = f"""
    **Role:** {role_description}

    **Your Task:** Write a compelling, psychologically-driven, and hyper-personalized email to the owner of a high-end landscaping business.

    **Core Principles:**
    - **Aspirational, Not Desperate:** Your tone is that of an expert consultant offering a valuable opportunity, not a salesperson begging for a meeting.
    - **Concise & Scannable:** The email must be short, direct, and easy to read on a phone. Aim for ~120 words.
    - **Opportunity-Vision-Path:** Frame the conversation around the opportunity to build a valuable digital asset.

    **Your Agency's Value Proposition:** We are a specialized agency that helps premium landscaping businesses dominate their local market on social media. We handle the complete process—**content strategy, professional editing, and consistent posting**—turning their Instagram into an automated system for attracting high-value clients.

    **Recipient Information:**
    - Business Name: {business_name}
    - Recipient's Likely Role(s): {titles if titles else "the Owner"}

    **Personalization Data:**
    - Positive Icebreaker (The Hook): "{icebreaker}"
    - Specific Evidence (The "Why You?"): "{specific_evidence}"

    **Instructions:**
    1.  Create a sharp, intriguing Subject Line. It should be professional and hint at brand building (e.g., "An idea for {business_name}", "Your work on Instagram", "Building the {business_name} brand").
    2.  Write the Email Body following the Opportunity-Vision-Path model:
        - **Opportunity:** Start with the **Positive Icebreaker** as a genuine, friendly opening.
        - **Vision:** Transition smoothly using the **Specific Evidence**. Paint a picture of what's possible—transforming their impressive work into a powerful Instagram presence that makes them the go-to authority for affluent homeowners in their area.
        - **Path:** Briefly and confidently state that your agency provides the end-to-end solution (strategy, editing, posting) to make this vision a reality.
    3.  **Low-Pressure CTA:** End with a confident, value-oriented call to action. Example: "Are you open to a brief call next week to discuss how a curated Instagram strategy could elevate the {business_name} brand?"
    4.  **Sign Off:** End with a professional closing.

    **Signature:**
    Daniel Laderman
    {persona_title}
    Lacad Consulting
    https://www.lacadconsulting.com

    **Output Format:** Provide the output as a JSON object with two keys: "subject" and "body".
    """

    try:
        print(f"Generating email for {business_name} with new expert persona...")
        response = openai.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=500,
            response_format={"type": "json_object"}
        )
        
        email_content = json.loads(response.choices[0].message.content)
        print(f"  > Success!")
        return email_content

    except Exception as e:
        print(f"🔴 Error generating email for {business_name}: {e}")
        return None

def generate_follow_up_email(prospect_data: dict, stage: int):
    """
    Generates a follow-up email based on the sequence stage.

    Args:
        prospect_data (dict): A dictionary containing all info for the prospect.
        stage (int): The follow-up stage (1, 2, or 3).

    Returns:
        dict: A dictionary containing the 'subject' and 'body' of the email.
    """
    business_name = prospect_data.get('name', 'your business')
    
    # Set the educational document link for the current strategy
    educational_doc_url = settings.CONTENT_STRATEGY_DOC_URL

    # --- Craft Email Content Based on Stage ---
    subject = ""
    body = ""

    # Define the content for the Instagram-focused strategy
    competitor_mention_stage1 = "To see what's possible, look at how competitors like FOXTERRA Design are leveraging social media to get millions of views on their projects (see their Instagram here - https://www.instagram.com/foxterradesign). This is the level of brand presence we aspire to build for our clients."
    competitor_mention_stage2 = "As we saw with FOXTERRA Design, a strong social media presence can be a game-changer."
    doc_description = "To give you some actionable advice, I've attached a brief guide on how a targeted content strategy can create a predictable stream of inbound leads for your business. It's a quick read with some of the key principles we use."

    if stage == 1:
        subject = f"Checking in re: {business_name}"
        body = f"""Hi,

I hope you're having a great week.

I'm writing to follow up on my previous email. {doc_description}

{competitor_mention_stage1}

You can view the guide here: {educational_doc_url}


Are you free for a quick chat next week to discuss how a similar strategy could work for {business_name}?

Best regards,
Daniel Laderman
Brand & Content Strategist | Lacad Consulting
https://www.lacadconsulting.com
"""

    elif stage == 2:
        subject = f"Some thoughts for {business_name}"
        body = f"""Hey {business_name},
Just wanted to quickly touch base one more time.. Here are some testimonials and references from companies we've worked with—feel free to check them out. Let me know if you have any questions.

Video Testimonials
Josh Webber - Webber Films - "https://vimeo.com/681122826"
Shye Lee and Aishah Mo - "https://vimeo.com/603391977"
Johnny Stephene, Dribble2Much - HandleLife - "https://vimeo.com/603391593"
Josh Perelin - "https://vimeo.com/603391669"

References
"https://pso-rite.com">Pso-Rite | "https://www.instagram.com/pso_rite/" - IG
"https://www.handlelife.com">HandleLife | "https://www.instagram.com/handlelife" - IG
"https://unlimitter.com">Unlimitter | "https://www.tiktok.com/@unlimitter" - TikTok | "https://www.instagram.com/getunlimitter/" - IG
"https://airelleskin.com">Airelle Skincare | "https://www.instagram.com/airelleskin/" - IG
"https://www.focusgts.com">Focus Global Talent Services | "https://www.instagram.com/focusglobaltalent/" - IG
"https://www.bloombeaconhr.com">Bloom & Beacon | "https://www.instagram.com/bloombeaconhr/" - IG

If turning your impressive work into a client-attracting social media brand is something you're considering, I'd be happy to share some initial thoughts. Let me know if you're open to a brief 15-minute call next week.


Respectfully,
Daniel Laderman
Brand & Content Strategist
Lacad Consulting
"https://www.lacadconsulting.com"


"""



    elif stage == 3:
        subject = "One last thing..."
        body = f"""Hi,

I understand that now might not be the best time to explore social media branding and marketing. I won't reach out again.

If you ever find yourself looking for ways to grow your client base, please don't hesitate to get in touch. We're always here to help.

Wishing you all the best with your business.

Best regards,
Daniel Laderman
Brand & Content Strategist | Lacad Consulting
https://www.lacadconsulting.com
"""

    else:
        return None # Invalid stage

    return {"subject": subject, "body": body}
