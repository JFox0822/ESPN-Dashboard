#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Data Fetcher
Pulls live league data and writes JSON files to /data for the GitHub Pages dashboard.
Runs daily via GitHub Actions.
"""

import requests
import json
import os
import sys
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
LEAGUE_ID = os.environ.get("ESPN_LEAGUE_ID", "163020")
SEASON    = 2026
MY_TEAM   = "Jacob"   # used to highlight your team in the dashboard

BASE_URL = (
    f"https://fantasy.espn.com/apis/v3/games/flb"
    f"/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}"
)

COOKIES = {}   # public league — no auth needed
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

# ESPN stat ID → readable label (H2H category names for your league)
STAT_MAP = {
    "20":  "R",
    "21":  "RBI",
    "5":   "HR",
    "23":  "SB",
    "27":  "K",       # batter strikeouts
    "2":   "AVG",
    "17":  "OPS",
    "34":  "IP",
    "41":  "H",       # hits allowed
    "48":  "K",       # pitcher strikeouts
    "63":  "QS",
    "47":  "ERA",
    "53":  "WHIP",
    "57":  "SV",
    "83":  "HLD",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch(*views):
    """GET the league endpoint with one or more ESPN view params."""
    params = [("view", v) for v in views]
    try:
        r = requests.get(BASE_URL, params=params, cookies=COOKIES, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        print(f"❌  HTTP {r.status_code} fetching views {views}: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌  Error fetching views {views}: {e}", file=sys.stderr)
        sys.exit(1)


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"🔄  Fetching ESPN data for league {LEAGUE_ID}, season {SEASON} …")
    updated = now_utc()

    # ── 1. Core data: teams, standings, settings ──────────────────────────────
    core = fetch("mStandings", "mTeam", "mSettings", "mStatus")

    scoring_period = core.get("scoringPeriodId", 1)
    current_week   = core.get("status", {}).get("currentMatchupPeriod", 1)

    teams_raw = core.get("teams", [])
    members   = core.get("members", [])

    # Member display-name lookup (owner names)
    member_map = {}
    for m in members:
        full = f"{m.get('firstName','')} {m.get('lastName','')}".strip()
        member_map[m["id"]] = full or m.get("displayName", m["id"])

    # Build team map
    team_map = {}
    for t in teams_raw:
        tid  = t["id"]
        name = f"{t.get('location','')} {t.get('nickname','')}".strip()
        owners = [member_map.get(o["id"], "") for o in t.get("owners", [])]
        team_map[tid] = {
            "id":     tid,
            "name":   name or t.get("abbrev", f"Team {tid}"),
            "abbrev": t.get("abbrev", f"T{tid}"),
            "owners": owners,
            "logo":   t.get("logo", ""),
        }

    # ── 2. Standings ──────────────────────────────────────────────────────────
    standings = []
    for t in teams_raw:
        rec = t.get("record", {}).get("overall", {})
        tm  = team_map[t["id"]]
        standings.append({
            **tm,
            "wins":          rec.get("wins", 0),
            "losses":        rec.get("losses", 0),
            "ties":          rec.get("ties", 0),
            "pointsFor":     round(rec.get("pointsFor", 0), 1),
            "pointsAgainst": round(rec.get("pointsAgainst", 0), 1),
            "streak":        t.get("record", {}).get("overall", {}).get("streakLength", 0),
            "streakType":    t.get("record", {}).get("overall", {}).get("streakType", ""),
            "seed":          t.get("playoffSeed", 0),
            "isMyTeam":      MY_TEAM.lower() in tm["name"].lower()
                             or any(MY_TEAM.lower() in o.lower() for o in tm["owners"]),
        })

    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1

    save("standings.json", {"week": current_week, "standings": standings, "updated": updated})

    # ── 3. Matchups (current week) ────────────────────────────────────────────
    sched_data = fetch("mMatchup", "mMatchupScore")
    schedule   = sched_data.get("schedule", [])

    current_matchups_raw = [
        m for m in schedule
        if m.get("matchupPeriodId") == current_week
    ]

    def parse_side(side):
        tid = side.get("teamId")
        cum = side.get("cumulativeScore", {})
        cat_raw = cum.get("scoreByStat", {})

        categories = {}
        for stat_id, info in cat_raw.items():
            label = STAT_MAP.get(stat_id, f"stat_{stat_id}")
            categories[label] = {
                "value":  round(info.get("score", 0), 3),
                "result": info.get("result", ""),   # WIN / LOSS / TIE
            }

        return {
            "teamId":   tid,
            "team":     team_map.get(tid, {}).get("name", f"Team {tid}"),
            "abbrev":   team_map.get(tid, {}).get("abbrev", ""),
            "catWins":  cum.get("wins", 0),
            "catLoss":  cum.get("losses", 0),
            "catTies":  cum.get("ties", 0),
            "categories": categories,
            "isMyTeam": team_map.get(tid, {}).get("isMyTeam", False)
                        if tid in team_map else False,
        }

    matchups_out = []
    for m in current_matchups_raw:
        home = parse_side(m.get("home", {}))
        away = parse_side(m.get("away", {}))
        # Determine leader
        if home["catWins"] > away["catWins"]:
            leader = home["team"]
        elif away["catWins"] > home["catWins"]:
            leader = away["team"]
        else:
            leader = "Tied"

        matchups_out.append({
            "home":      home,
            "away":      away,
            "leader":    leader,
            "winner":    m.get("winner", "UNDECIDED"),
        })

    save("matchups.json", {
        "week":     current_week,
        "period":   scoring_period,
        "matchups": matchups_out,
        "updated":  updated,
    })

    # ── 4. Team Stats (season cumulative) ────────────────────────────────────
    team_stats = []
    for t in teams_raw:
        stat_totals = t.get("valuesByStat", {})
        readable = {}
        for sid, val in stat_totals.items():
            label = STAT_MAP.get(str(sid), f"stat_{sid}")
            readable[label] = round(val, 3) if isinstance(val, float) else val

        tm = team_map[t["id"]]
        rec = t.get("record", {}).get("overall", {})
        team_stats.append({
            **tm,
            "wins":   rec.get("wins", 0),
            "losses": rec.get("losses", 0),
            "stats":  readable,
        })

    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── 5. Power Rankings (record-vs-everyone for current week) ───────────────
    # Each team's score vs every other team's score this week → synthetic W/L
    week_scores = {}
    for m in current_matchups_raw:
        for side_key in ("home", "away"):
            side = m.get(side_key, {})
            tid  = side.get("teamId")
            cum  = side.get("cumulativeScore", {})
            if tid is not None:
                week_scores[tid] = cum.get("wins", 0)   # category wins = "score"

    power = []
    for tid, score in week_scores.items():
        pw = pl = pt = 0
        for other_id, other_score in week_scores.items():
            if other_id == tid:
                continue
            if score > other_score:   pw += 1
            elif score < other_score: pl += 1
            else:                     pt += 1

        tm   = team_map.get(tid, {})
        s_rec = next((s for s in standings if s["id"] == tid), {})
        power.append({
            "id":          tid,
            "name":        tm.get("name", f"Team {tid}"),
            "abbrev":      tm.get("abbrev", ""),
            "isMyTeam":    tm.get("isMyTeam", False),
            "overallW":    s_rec.get("wins", 0),
            "overallL":    s_rec.get("losses", 0),
            "overallRank": s_rec.get("rank", 0),
            "pwWins":      pw,
            "pwLosses":    pl,
            "pwTies":      pt,
            "weekScore":   score,
        })

    power.sort(key=lambda x: (-x["pwWins"], -x["weekScore"]))
    for i, p in enumerate(power):
        p["pwRank"] = i + 1
        p["rankDelta"] = p["overallRank"] - p["pwRank"]   # + means PR rank is better

    save("power_rankings.json", {
        "week":     current_week,
        "rankings": power,
        "updated":  updated,
    })

    # ── 6. Meta (league info for the dashboard header) ────────────────────────
    settings = core.get("settings", {})
    save("meta.json", {
        "leagueName":    settings.get("name", "The League"),
        "season":        SEASON,
        "currentWeek":   current_week,
        "scoringPeriod": scoring_period,
        "teamCount":     len(teams_raw),
        "myTeam":        MY_TEAM,
        "updated":       updated,
    })

    print(f"\n🏆  Done! Week {current_week}, scoring period {scoring_period}.")


if __name__ == "__main__":
    main()
