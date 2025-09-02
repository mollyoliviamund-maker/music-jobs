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

