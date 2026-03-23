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
LEAGUE_ID  = os.environ.get("ESPN_LEAGUE_ID", "163020")
ESPN_S2    = os.environ.get("ESPN_S2", "")
ESPN_SWID  = os.environ.get("ESPN_SWID", "")
SEASON     = 2026

BASE_URL = (
    f"https://fantasy.espn.com/apis/v3/games/flb"
    f"/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}"
)

# Only send cookies when provided; sending empty strings can cause 403 on some leagues
COOKIES = {}
if ESPN_S2:
    COOKIES["espn_s2"] = ESPN_S2
if ESPN_SWID:
    COOKIES["SWID"] = ESPN_SWID
HEADERS = {"Accept": "application/json"}

# ESPN stat ID → readable label
STAT_MAP = {
    "20":  "R",
    "21":  "RBI",
    "5":   "HR",
    "23":  "SB",
    "27":  "K",       # batter Ks
    "2":   "AVG",
    "17":  "OPS",
    "34":  "IP",
    "41":  "H",       # hits allowed
    "48":  "K",       # pitcher Ks (overwrites batter K in combined view)
    "63":  "QS",
    "47":  "ERA",
    "53":  "WHIP",
    "57":  "SV",
    "83":  "HLD",
}

# ESPN lineup slot ID → display label (where player is SLOTTED)
SLOT_MAP = {
    0:  "C",      1:  "1B",    2:  "2B",    3:  "3B",   4:  "SS",
    5:  "OF",     6:  "2B/SS", 7:  "1B/3B", 8:  "LF",   9:  "CF",
    10: "RF",     11: "DH",    12: "UTIL",  13: "SP",    14: "RP",
    15: "P",      16: "BE",    17: "IL",    18: "IL10",  19: "IL60",
    20: "NA",     21: "BE",    22: "IL",
}

# ESPN defaultPositionId → player's actual primary position
POS_MAP = {
    1:  "C",     2:  "1B",   3:  "2B",   4:  "3B",   5:  "SS",
    6:  "OF",    7:  "2B/SS",8:  "1B/3B",9:  "P",    10: "SP",
    11: "RP",    12: "DH",   13: "P",    14: "UTIL",
}

# Lower-is-better pitching stats
LOWER_BETTER = {"ERA", "WHIP", "H"}

# ── Helpers ───────────────────────────────────────────────────────────────────
def fetch(*views):
    params = [("view", v) for v in views]
    try:
        r = requests.get(BASE_URL, params=params, cookies=COOKIES, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        print(f"❌  HTTP {r.status_code} fetching views {views}: {e}", file=sys.stderr)
        if r.status_code == 401:
            print("   → League may be private. Add ESPN_S2 and SWID as GitHub Secrets.", file=sys.stderr)
        elif r.status_code == 403:
            print("   → ESPN is blocking the request. Trying without cookies...", file=sys.stderr)
            # Retry once without any cookies
            try:
                r2 = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
                r2.raise_for_status()
                return r2.json()
            except Exception as e2:
                print(f"   → Retry also failed: {e2}", file=sys.stderr)
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

    # ── 1. Core data ──────────────────────────────────────────────────────────
    core = fetch("mStandings", "mTeam", "mSettings", "mStatus")

    scoring_period = core.get("scoringPeriodId", 1)
    current_week   = core.get("status", {}).get("currentMatchupPeriod", 1)
    teams_raw      = core.get("teams", [])
    members        = core.get("members", [])

    member_map = {}
    for m in members:
        full = f"{m.get('firstName','')} {m.get('lastName','')}".strip()
        member_map[m["id"]] = full or m.get("displayName", m["id"])

    team_map = {}
    for t in teams_raw:
        tid    = t["id"]
        name   = f"{t.get('location','')} {t.get('nickname','')}".strip()
        owners = [member_map.get(o["id"], "") for o in t.get("owners", [])]
        abbrev = t.get("abbrev", f"T{tid}")
        team_map[tid] = {
            "id":     tid,
            "name":   name or abbrev,
            "abbrev": abbrev,
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
            "streak":        rec.get("streakLength", 0),
            "streakType":    rec.get("streakType", ""),
            "seed":          t.get("playoffSeed", 0),
        })

    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1

    save("standings.json", {"week": current_week, "standings": standings, "updated": updated})

    # ── 3. Matchups (current week) ────────────────────────────────────────────
    sched_data = fetch("mMatchup", "mMatchupScore")
    schedule   = sched_data.get("schedule", [])
    current_matchups_raw = [m for m in schedule if m.get("matchupPeriodId") == current_week]

    def parse_side(side):
        tid = side.get("teamId")
        cum = side.get("cumulativeScore", {})
        cat_raw = cum.get("scoreByStat", {})
        categories = {}
        for stat_id, info in cat_raw.items():
            label = STAT_MAP.get(stat_id, f"stat_{stat_id}")
            categories[label] = {
                "value":  round(info.get("score", 0), 3),
                "result": info.get("result", ""),
            }
        return {
            "teamId":     tid,
            "team":       team_map.get(tid, {}).get("name", f"Team {tid}"),
            "abbrev":     team_map.get(tid, {}).get("abbrev", ""),
            "catWins":    cum.get("wins", 0),
            "catLoss":    cum.get("losses", 0),
            "catTies":    cum.get("ties", 0),
            "categories": categories,
        }

    matchups_out = []
    for m in current_matchups_raw:
        home = parse_side(m.get("home", {}))
        away = parse_side(m.get("away", {}))
        hw = home["catWins"] > away["catWins"]
        aw = away["catWins"] > home["catWins"]
        matchups_out.append({
            "home":   home,
            "away":   away,
            "leader": home["team"] if hw else (away["team"] if aw else "Tied"),
            "winner": m.get("winner", "UNDECIDED"),
        })

    save("matchups.json", {"week": current_week, "period": scoring_period, "matchups": matchups_out, "updated": updated})

    # ── 4. Team Stats (season cumulative) ────────────────────────────────────
    team_stats = []
    for t in teams_raw:
        stat_totals = t.get("valuesByStat", {})
        readable = {}
        for sid, val in stat_totals.items():
            lbl = STAT_MAP.get(str(sid))
            if lbl:
                readable[lbl] = round(val, 3) if isinstance(val, float) else val
        tm = team_map[t["id"]]
        s_rec = next((s for s in standings if s["id"] == t["id"]), {})
        team_stats.append({
            **tm,
            "wins":   s_rec.get("wins", 0),
            "losses": s_rec.get("losses", 0),
            "stats":  readable,
        })

    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── 5. Rosters ────────────────────────────────────────────────────────────
    print("  📋  Fetching rosters …")
    try:
        ros_data = fetch("mRoster")
        ros_teams_raw = ros_data.get("teams", [])

        # Determine stat split key for 2026 actuals
        stat_key_actual = f"00{SEASON}"     # e.g. "002026"
        stat_key_proj   = f"01{SEASON}"     # e.g. "012026"

        rosters_out = []
        for t in ros_teams_raw:
            tid = t["id"]
            tm  = team_map.get(tid, {})
            entries = t.get("roster", {}).get("entries", [])
            players = []
            for entry in entries:
                lineup_slot_id = entry.get("lineupSlotId", 16)
                slot_label     = SLOT_MAP.get(lineup_slot_id, "BE")

                ppe    = entry.get("playerPoolEntry", {})
                player = ppe.get("player", {})
                pname  = player.get("fullName", "Unknown")

                # Primary position from defaultPositionId
                default_pos_id = player.get("defaultPositionId", 0)
                primary_pos    = POS_MAP.get(default_pos_id, "?")

                # Eligible positions list (e.g. ["SP", "RP"])
                eligible_ids   = player.get("eligibleSlots", [])
                eligible_pos   = [SLOT_MAP.get(s, "") for s in eligible_ids if SLOT_MAP.get(s, "") not in ("BE","IL","IL10","IL60","NA","")]
                eligible_str   = "/".join(dict.fromkeys(eligible_pos))[:20]

                is_pitcher     = default_pos_id in (9, 10, 11, 13)
                inj_status     = ppe.get("injuryStatus", "ACTIVE")

                # Stats — prefer actuals, fall back to projections
                stats_raw = {}
                for s_entry in ppe.get("acquisitionType", {}):
                    pass  # skip
                # stats are keyed under player.stats list
                stat_map_out = {}
                for s_obj in player.get("stats", []):
                    split = str(s_obj.get("id", ""))  # e.g. "002026"
                    vals  = s_obj.get("stats", {})
                    if split == stat_key_actual:
                        for sid, val in vals.items():
                            lbl = STAT_MAP.get(str(sid))
                            if lbl:
                                stat_map_out[lbl] = round(val, 3) if isinstance(val, float) else val
                        break
                # If no actuals, try projections
                if not stat_map_out:
                    for s_obj in player.get("stats", []):
                        split = str(s_obj.get("id", ""))
                        if split == stat_key_proj:
                            vals = s_obj.get("stats", {})
                            for sid, val in vals.items():
                                lbl = STAT_MAP.get(str(sid))
                                if lbl:
                                    stat_map_out[lbl] = round(val, 3) if isinstance(val, float) else val
                            break

                # Pro-rate score for tier classification
                hit_cats  = ["R", "RBI", "HR", "SB", "AVG", "OPS"]
                pit_cats  = ["IP", "QS", "ERA", "WHIP", "K", "SV"]
                hit_score = 0.0
                pit_score = 0.0

                players.append({
                    "name":        pname,
                    "slot":        slot_label,
                    "position":    primary_pos,
                    "eligible":    eligible_str,
                    "isPitcher":   is_pitcher,
                    "injStatus":   inj_status,
                    "stats":       stat_map_out,
                    "hitScore":    hit_score,
                    "pitchScore":  pit_score,
                    "tier":        "",
                })

            rosters_out.append({
                **tm,
                "teamId":  tid,
                "players": players,
            })

        save("rosters.json", {
            "season":    SEASON,
            "week":      current_week,
            "teams":     rosters_out,
            "projBlend": {"projPct": 0, "actualPct": 100},
            "updated":   updated,
        })
    except Exception as e:
        print(f"  ⚠️  Roster fetch failed: {e}", file=sys.stderr)
        save("rosters.json", {"season": SEASON, "week": current_week, "teams": [], "updated": updated})

    # ── 6. Power Rankings ─────────────────────────────────────────────────────
    week_scores = {}
    for m in current_matchups_raw:
        for side_key in ("home", "away"):
            side = m.get(side_key, {})
            tid  = side.get("teamId")
            cum  = side.get("cumulativeScore", {})
            if tid is not None:
                week_scores[tid] = cum.get("wins", 0)

    power = []
    for tid, score in week_scores.items():
        pw = pl = pt = 0
        for other_id, other_score in week_scores.items():
            if other_id == tid: continue
            if score > other_score:   pw += 1
            elif score < other_score: pl += 1
            else:                     pt += 1

        tm     = team_map.get(tid, {})
        s_rec  = next((s for s in standings if s["id"] == tid), {})
        t_stats = next((t for t in team_stats if t["id"] == tid), {})
        power.append({
            "id":          tid,
            "name":        tm.get("name", f"Team {tid}"),
            "abbrev":      tm.get("abbrev", ""),
            "overallW":    s_rec.get("wins", 0),
            "overallL":    s_rec.get("losses", 0),
            "overallRank": s_rec.get("rank", 0),
            "h2hWins":     s_rec.get("wins", 0),
            "h2hLosses":   s_rec.get("losses", 0),
            "pwWins":      pw,
            "pwLosses":    pl,
            "weekScore":   score,
            "composite":   round(pw / max(pw + pl, 1) * 100, 1),
            "catRanks":    {},  # populated below
        })

    # Compute per-category ranks
    all_cats = ["R", "RBI", "HR", "SB", "AVG", "OPS", "IP", "H", "K", "QS", "ERA", "WHIP", "SV", "HLD"]
    for cat in all_cats:
        vals = [(p["id"], team_stats_map := {t["id"]: t.get("stats", {}) for t in team_stats},
                 team_stats_map.get(p["id"], {}).get(cat)) for p in power]
        # simpler:
        cat_vals = []
        ts_map = {t["id"]: t.get("stats", {}) for t in team_stats}
        for p in power:
            v = ts_map.get(p["id"], {}).get(cat)
            if v is not None:
                cat_vals.append((p["id"], v))
        if len(cat_vals) < 2:
            continue
        lower = cat in LOWER_BETTER
        cat_vals.sort(key=lambda x: x[1], reverse=not lower)
        for rank, (pid, _) in enumerate(cat_vals, 1):
            for p in power:
                if p["id"] == pid:
                    p["catRanks"][cat] = rank

    power.sort(key=lambda x: (-x["pwWins"], -x["weekScore"]))
    for i, p in enumerate(power):
        p["pwRank"]    = i + 1
        p["rankDelta"] = p["overallRank"] - p["pwRank"]

    save("power_rankings.json", {"week": current_week, "rankings": power, "updated": updated})

    # ── 7. History ────────────────────────────────────────────────────────────
    # All-time H2H matrix from schedule
    all_matchups = [m for m in schedule if m.get("winner") not in ("UNDECIDED", None)]
    h2h_records = {}
    for m in all_matchups:
        home_id = m.get("home", {}).get("teamId")
        away_id = m.get("away", {}).get("teamId")
        winner  = m.get("winner", "")
        if home_id is None or away_id is None:
            continue
        key = tuple(sorted([home_id, away_id]))
        if key not in h2h_records:
            h2h_records[key] = {"t1": key[0], "t2": key[1], "t1w": 0, "t2w": 0, "ties": 0}
        rec = h2h_records[key]
        if winner == "HOME":
            (rec["t1w"] if home_id == key[0] else rec["t2w"])
            if home_id == key[0]: rec["t1w"] += 1
            else: rec["t2w"] += 1
        elif winner == "AWAY":
            if away_id == key[0]: rec["t1w"] += 1
            else: rec["t2w"] += 1
        else:
            rec["ties"] += 1

    h2h_list = []
    for rec in h2h_records.values():
        t1 = team_map.get(rec["t1"], {}).get("name", f"Team {rec['t1']}")
        t2 = team_map.get(rec["t2"], {}).get("name", f"Team {rec['t2']}")
        h2h_list.append({"team1": t1, "team2": t2, "t1w": rec["t1w"], "t2w": rec["t2w"], "ties": rec["ties"]})

    save("history_h2h.json", {"records": h2h_list, "updated": updated})

    # ── 8. Meta ───────────────────────────────────────────────────────────────
    settings = core.get("settings", {})
    team_list = [{"id": t["id"], "name": team_map[t["id"]]["name"], "abbrev": team_map[t["id"]]["abbrev"]} for t in teams_raw]
    save("meta.json", {
        "leagueName":    settings.get("name", "The League"),
        "season":        SEASON,
        "currentWeek":   current_week,
        "scoringPeriod": scoring_period,
        "teamCount":     len(teams_raw),
        "teams":         team_list,
        "updated":       updated,
    })

    print(f"\n🏆  Done! Week {current_week}, scoring period {scoring_period}.")


if __name__ == "__main__":
    main()
