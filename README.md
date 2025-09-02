# Music Job Watcher

This project watches selected education and assessment companies for job postings
that mention **"Music"** in the title or description. It currently supports
companies hosted on **Greenhouse** and **Lever** job boards.

Results are stored in:
- `music_jobs.csv` → CSV log of all matching jobs
- `seen_music.json` → deduplication store so the same posting isn’t added twice

The workflow is configured to run on GitHub Actions twice per day, but you can
also run it locally.

---

## 🔧 Setup (Local)

1. Clone the repo:
   ```bash
   git clone https://github.com/<your-username>/music-jobs.git
   cd music-jobs
Create a virtual environment and install dependencies:

bash
Copy code
python -m venv .venv
# Windows PowerShell:
. .venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
Run the scraper:

bash
Copy code
python music_scraper.py
Optional targeting for debugging:

bash
Copy code
python music_scraper.py --platform greenhouse --company duolingo
python music_scraper.py --platform lever --company udacity
🏗️ Configure Companies
Edit companies.yaml to add or remove companies. Each entry must match the slug
used on its job board:

Greenhouse: https://boards.greenhouse.io/<slug>

Lever: https://jobs.lever.co/<slug>

If a slug is wrong, the scraper will warn in the logs.

🚀 GitHub Actions
This repo includes .github/workflows/scrape.yml which:

Runs the scraper twice daily (times are in UTC).

Commits updates to music_jobs.csv and seen_music.json.

You can also run it manually from the Actions tab.

📧 Optional Email Notifications
If you want the workflow to email results:

Generate a Gmail App Password (16 characters).

Add secrets in the repo under Settings → Secrets and variables → Actions:

EMAIL_ADDRESS

EMAIL_PASSWORD

Extend the workflow to send email (see comments in scrape.yml).

⚠️ Limitations
Only Greenhouse and Lever are supported right now. Many large vendors
(Pearson, ETS, ACT, etc.) use Workday or iCIMS; adapters for those will be
added separately.

“Music” matches are case-insensitive but simple substring matches. False
positives may occur if “music” appears in unrelated text.
