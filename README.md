# AI-Powered Hyper-Personalized Outreach System for LAC Advertisement

This project is an end-to-end AI-driven system that identifies suitable landscaping businesses, analyzes their public reviews and online presence for pain points related to lead generation, online visibility, and conversion, and crafts highly personalized cold emails offering LAC Advertisement's specific digital marketing solutions. The system aims to book qualified calls efficiently.

## Project Structure

- `main.py`: The main entry point for the application.
- `src/`: Contains the core source code for the project.
- `data/`: Used for storing data like CSVs, JSON files, etc.
- `config/`: For configuration files.
- `tests/`: Contains tests for the application code.
- `api/`: Contains the web server for the remote control UI.
- `templates/`: Contains the HTML/JS for the remote control UI.
- `requirements.txt`: Lists the project's Python dependencies.
- `.env`: For storing environment variables, including sensitive API keys.
- `.env.example`: An example of the `.env` file.

## Getting Started

Follow these instructions to set up and run the project on your local machine.

### 1. Clone the Repository

```bash
git clone https://github.com/danielladerman/ai_landscape_agent.git
cd ai_landscape_agent
```

### 2. Set Up a Virtual Environment

It is highly recommended to use a virtual environment to manage project dependencies.

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a file named `.env` in the root of the project and add the following environment variables. Fill in the values with your actual keys and settings.

```
OPENAI_API_KEY="your_openai_api_key"
GOOGLE_MAPS_API_KEY="your_google_maps_api_key"
SPREADSHEET_ID="your_google_spreadsheet_id"
GOOGLE_SHEET_NAME="your_sheet_name" # e.g., Sheet1
WEB_API_KEY="a_secure_secret_key_for_the_web_ui"
```

### 5. Add Google Credentials

You will need three credential files to authenticate with Google APIs (Sheets and Gmail). Place the following files in the root of the project directory:

-   `credentials.json`
-   `token.json`
-   `google_credentials.json`

### 6. Run the Application

You can run the core scripts directly or use the web-based command center.

**To run a script:**

```bash
python build_prospect_list.py "landscaping in San Diego" --max_leads 10
```

**To start the web command center:**

```bash
python api/index.py
```
Then, open your browser to `http://127.0.0.1:8000`.

## Deployment to Render

The application is configured for manual deployment on Render as a Web Service.

### 1. Create a New Web Service

-   In the Render dashboard, click **New +** > **Web Service**.
-   Connect your Git repository.

### 2. Configure the Service Settings

-   **Name**: A name for your service (e.g., `ai-outreach-command-center`).
-   **Region**: Your preferred region (e.g., `Oregon (US West)`).
-   **Branch**: `main`.
-   **Runtime**: `Python 3`.
-   **Build Command**: `pip install gunicorn && pip install -r requirements.txt`
-   **Start Command**: `python -m gunicorn api.index:app --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT`
-   **Instance Type**: `Free`.

### 3. Add Environment Variables

In the **Environment** section, add all the variables from your local `.env` file (e.g., `OPENAI_API_KEY`, `WEB_API_KEY`, etc.).

### 4. Add Secret Files

In the **Secret Files** section, add the following three files. The **Filename** should be just the name of the file, and the **Contents** should be the full JSON content from your local files.

-   **Filename**: `credentials.json`
-   **Filename**: `token.json`
-   **Filename**: `google_credentials.json`

After saving, Render will deploy your application.

Let's do a quick recap of what we've built together:
A Scalable System: We have a robust project structure with two main scripts: build_prospect_list.py to do the heavy lifting of research and AI generation, and run_daily_sending.py to handle the actual outreach in a controlled way.
Ethical Lead Generation: The system uses the official Google Maps API to find businesses and then intelligently scrapes their public websites for contact information.
Deep Analysis: It goes far beyond simple scraping. It analyzes website content (blogs, CTAs, social links), gets concrete performance metrics from the Google PageSpeed API, and can even perform sentiment analysis on Google Reviews.
Hyper-Personalized AI: The core of the system is the AI email generator which doesn't just use a generic template. It dynamically assigns one of four expert AI personas based on the specific pain point it discovered, ensuring the outreach is incredibly relevant and compelling.
Ready for Production: We've debugged API access, handled real-world errors like timeouts, and structured the system to be something you can confidently implement for your agency.