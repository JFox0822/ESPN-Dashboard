name: Daily ESPN Data Refresh

on:
  schedule:
    # Runs at 8:00 AM UTC (4 AM ET) every day during baseball season
    - cron: "0 8 * * *"
  workflow_dispatch:   # also allows manual runs from the Actions tab

permissions:
  contents: write

jobs:
  fetch-and-deploy:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: pip install requests espn-api

      - name: Fetch ESPN data
        run: python fetch_espn.py

      - name: Commit and push updated data
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          # Only commit if there are actual changes
          git diff --cached --quiet && echo "No changes to commit" || \
            git commit -m "chore: refresh ESPN data $(date -u +'%Y-%m-%d %H:%M UTC')"
          git push
