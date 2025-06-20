# AI-Powered Hyper-Personalized Outreach System for LAC Advertisement

This project is an end-to-end AI-driven system that identifies suitable landscaping businesses, analyzes their public reviews and online presence for pain points related to lead generation, online visibility, and conversion, and crafts highly personalized cold emails offering LAC Advertisement's specific digital marketing solutions. The system aims to book qualified calls efficiently.

## Project Structure

- `main.py`: The main entry point for the application.
- `src/`: Contains the core source code for the project.
- `data/`: Used for storing data like CSVs, JSON files, etc.
- `config/`: For configuration files.
- `tests/`: Contains tests for the application code.
- `venv/`: The Python virtual environment directory.
- `requirements.txt`: Lists the project's Python dependencies.
- `.env`: For storing environment variables, including sensitive API keys.
- `.env.example`: An example of the `.env` file.

Let's do a quick recap of what we've built together:
A Scalable System: We have a robust project structure with two main scripts: build_prospect_list.py to do the heavy lifting of research and AI generation, and run_daily_sending.py to handle the actual outreach in a controlled way.
Ethical Lead Generation: The system uses the official Google Maps API to find businesses and then intelligently scrapes their public websites for contact information.
Deep Analysis: It goes far beyond simple scraping. It analyzes website content (blogs, CTAs, social links), gets concrete performance metrics from the Google PageSpeed API, and can even perform sentiment analysis on Google Reviews.
Hyper-Personalized AI: The core of the system is the AI email generator which doesn't just use a generic template. It dynamically assigns one of four expert AI personas based on the specific pain point it discovered, ensuring the outreach is incredibly relevant and compelling.
Ready for Production: We've debugged API access, handled real-world errors like timeouts, and structured the system to be something you can confidently implement for your agency.