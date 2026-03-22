#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Full Data Fetcher
Pulls live + historical league data and writes JSON files to /data/.
Runs daily via GitHub Actions.
"""

import requests
import json
import os
import sys
import time
import math
from datetime import datetime, timezone
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
LEAGUE_ID       = os.environ.get("ESPN_LEAGUE_ID", "163020")
CURRENT_SEASON  = 2026
MY_TEAM         = "Jacob"
HISTORY_SEASONS = list(range(2019, 2026))
PLAYOFF_SPOTS   = 6

HEADERS = {
    "Accept": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

# ESPN stat ID → label
STAT_MAP = {
    "20":"R",  "21":"RBI","5":"HR",  "23":"SB",
    "27":"K",  "2":"AVG", "17":"OPS","34":"IP",
    "41":"H",  "48":"K",  "63":"QS", "47":"ERA",
    "53":"WHIP","57":"SV","83":"HLD",
}

HIT_CATS   = ["R","RBI","HR","SB","K","AVG","OPS"]
PITCH_CATS = ["IP","H","K","QS","ERA","WHIP","SV"]
ALL_CATS   = HIT_CATS + PITCH_CATS

# Lower is better (affects category ranking direction)
LOWER_IS_BETTER = {"ERA","WHIP","H"}

# ESPN lineup slot ID classification
ACTIVE_HIT_SLOTS   = {0,1,2,3,4,5,6,7,10}        # C,1B,2B,3B,SS,OF×3,DH
ACTIVE_PITCH_SLOTS = {11,12,13,14,15,16}           # SP×4, RP×2
BENCH_SLOTS        = {17,18,19,20}
IL_SLOTS           = {21}

# ESPN default position ID classification (for bench player typing)
PITCHER_POS_IDS = {1, 11, 14}   # SP, RP, P
HITTER_POS_IDS  = {2,3,4,5,6,7,8,9,10,12}  # C,1B,2B,3B,SS,OF×3,DH,OF

# Hitting stats used for player z-score scoring (higher = better)
HIT_SCORE_STATS   = ["R","RBI","HR","SB","AVG","OPS"]
# Pitching stats: sign indicates direction (+1 higher better, -1 lower better)
PITCH_SCORE_STATS = {
    "IP":+1, "K":+1, "QS":+1, "SV":+1, "HLD":+1,
    "ERA":-1, "WHIP":-1, "H":-1,
}

# ── Composite PR weights ──────────────────────────────────────────────────────
W_CAT_RANK     = 0.30
W_STARTER_HIT  = 0.25
W_STARTER_PIT  = 0.25
W_BENCH_HIT    = 0.10
W_BENCH_PIT    = 0.10

# ── Helpers ───────────────────────────────────────────────────────────────────
def base_url(season):
    return (f"https://fantasy.espn.com/apis/v3/games/flb"
            f"/seasons/{season}/segments/0/leagues/{LEAGUE_ID}")

def fetch(season, *views, silent=False):
    params = [("view", v) for v in views]
    if not silent:
        print(f"    ESPN [{season}] views={views}")
    try:
        r = requests.get(base_url(season), params=params,
                         cookies={}, headers=HEADERS, timeout=30)
        if not silent:
            print(f"    → {r.status_code}")
        if r.status_code != 200:
            print(f"    ⚠ HTTP {r.status_code}: {r.text[:300]}")
            return None
        try:
            return r.json()
        except Exception:
            print(f"    ⚠ JSON parse error. Response: {r.text[:300]}")
            return None
    except Exception as e:
        print(f"    ⚠ Request error: {e}")
        return None

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

def build_team_map(teams_raw, members):
    member_map = {}
    for m in members:
        full = f"{m.get('firstName','')} {m.get('lastName','')}".strip()
        member_map[m["id"]] = full or m.get("displayName", m["id"])
    team_map = {}
    for t in teams_raw:
        tid   = t["id"]
        name  = f"{t.get('location','')} {t.get('nickname','')}".strip()
        owners = [member_map.get(o["id"], "") for o in t.get("owners", [])]
        is_mine = (MY_TEAM.lower() in name.lower()
                   or any(MY_TEAM.lower() in o.lower() for o in owners))
        team_map[tid] = {
            "id":tid,"name":name or t.get("abbrev",f"Team {tid}"),
            "abbrev":t.get("abbrev",f"T{tid}"),
            "owners":owners,"logo":t.get("logo",""),"isMyTeam":is_mine,
        }
    return team_map

# ── Z-score helpers ───────────────────────────────────────────────────────────
def mean_std(values):
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0, 1.0
    m = sum(vals) / len(vals)
    variance = sum((v - m) ** 2 for v in vals) / len(vals)
    return m, math.sqrt(variance) if variance > 0 else 1.0

def compute_player_scores(all_players):
    """
    Given a flat list of player dicts (each with 'stats': {label: value}),
    compute z-score-based hitting and pitching scores.
    Returns a dict keyed by player index → {hitScore, pitchScore}.
    """
    # Collect per-stat values across all players
    hit_vals   = {s: [] for s in HIT_SCORE_STATS}
    pitch_vals = {s: [] for s in PITCH_SCORE_STATS}

    for p in all_players:
        stats = p.get("stats", {})
        for s in HIT_SCORE_STATS:
            v = stats.get(s)
            if v is not None:
                hit_vals[s].append(v)
        for s in PITCH_SCORE_STATS:
            v = stats.get(s)
            if v is not None:
                pitch_vals[s].append(v)

    # Compute mean/std per stat
    hit_norm   = {s: mean_std(hit_vals[s])   for s in HIT_SCORE_STATS}
    pitch_norm = {s: mean_std(pitch_vals[s]) for s in PITCH_SCORE_STATS}

    scores = []
    for p in all_players:
        stats = p.get("stats", {})

        # Hitting z-score: sum of z-scores across hitting stats
        h_zs = []
        for s in HIT_SCORE_STATS:
            v = stats.get(s)
            if v is not None:
                m, sd = hit_norm[s]
                h_zs.append((v - m) / sd)
        hit_score = sum(h_zs) / len(h_zs) if h_zs else 0.0

        # Pitching z-score: sum of signed z-scores across pitching stats
        p_zs = []
        for s, direction in PITCH_SCORE_STATS.items():
            v = stats.get(s)
            if v is not None:
                m, sd = pitch_norm[s]
                p_zs.append(direction * (v - m) / sd)
        pitch_score = sum(p_zs) / len(p_zs) if p_zs else 0.0

        scores.append({"hitScore": hit_score, "pitchScore": pitch_score})

    return scores

def normalize_to_100(values):
    """Map a list of floats to 0-100, higher = better."""
    vals = list(values)
    mn, mx = min(vals), max(vals)
    if mx == mn:
        return [50.0] * len(vals)
    return [round((v - mn) / (mx - mn) * 100, 1) for v in vals]

# ── CURRENT SEASON ────────────────────────────────────────────────────────────
def fetch_current_season():
    print("\n━━ Current Season ━━")
    updated = now_utc()

    core = fetch(CURRENT_SEASON, "mTeam", "mStandings")
    if not core:
        print("❌  Could not fetch core data"); sys.exit(1)

    scoring_period = core.get("scoringPeriodId", 1)
    current_week   = core.get("status", {}).get("currentMatchupPeriod", 1)
    teams_raw      = core.get("teams", [])
    members        = core.get("members", [])
    team_map       = build_team_map(teams_raw, members)
    print(f"  → {len(teams_raw)} teams | week {current_week} | period {scoring_period}")

    # ── Standings ─────────────────────────────────────────────────────────────
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
    save("standings.json", {"week":current_week,"standings":standings,"updated":updated})

    # ── Matchups ──────────────────────────────────────────────────────────────
    sched_data = fetch(CURRENT_SEASON, "mMatchup", "mMatchupScore")
    schedule   = sched_data.get("schedule", []) if sched_data else []
    current_matchups_raw = [m for m in schedule if m.get("matchupPeriodId") == current_week]
    print(f"  → {len(current_matchups_raw)} matchups this week")

    def parse_side(side):
        tid = side.get("teamId")
        cum = side.get("cumulativeScore", {})
        categories = {}
        for stat_id, info in cum.get("scoreByStat", {}).items():
            label = STAT_MAP.get(stat_id, f"s{stat_id}")
            categories[label] = {"value":round(info.get("score",0),3),"result":info.get("result","")}
        return {
            "teamId":tid,"team":team_map.get(tid,{}).get("name",f"Team {tid}"),
            "abbrev":team_map.get(tid,{}).get("abbrev",""),
            "catWins":cum.get("wins",0),"catLoss":cum.get("losses",0),"catTies":cum.get("ties",0),
            "categories":categories,"isMyTeam":team_map.get(tid,{}).get("isMyTeam",False),
        }

    matchups_out = []
    for m in current_matchups_raw:
        home = parse_side(m.get("home", {}))
        away = parse_side(m.get("away", {}))
        leader = home["team"] if home["catWins"]>away["catWins"] else \
                 away["team"] if away["catWins"]>home["catWins"] else "Tied"
        matchups_out.append({"home":home,"away":away,"leader":leader,"winner":m.get("winner","UNDECIDED")})
    save("matchups.json",{"week":current_week,"period":scoring_period,"matchups":matchups_out,"updated":updated})

    # ── Team Stats ────────────────────────────────────────────────────────────
    team_stats  = []
    cat_values  = defaultdict(dict)   # cat → {tid: value}

    for t in teams_raw:
        stat_totals = t.get("valuesByStat", {})
        readable = {}
        for sid, val in stat_totals.items():
            label = STAT_MAP.get(str(sid), f"s{sid}")
            v = round(val, 3) if isinstance(val, float) else val
            readable[label] = v
            cat_values[label][t["id"]] = v
        tm  = team_map[t["id"]]
        rec = t.get("record", {}).get("overall", {})
        team_stats.append({**tm,"wins":rec.get("wins",0),"losses":rec.get("losses",0),"stats":readable})

    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json",{"season":CURRENT_SEASON,"teams":team_stats,"updated":updated})

    # ── Rosters + Player Scoring ──────────────────────────────────────────────
    print("  → Fetching rosters …")
    roster_data = fetch(CURRENT_SEASON, "mRoster")

    pos_map = {1:"SP",2:"C",3:"1B",4:"2B",5:"3B",6:"SS",7:"LF",8:"CF",9:"RF",
               10:"DH",11:"RP",12:"OF",14:"P",16:"BE",17:"IL",0:"n/a"}
    slot_map = {0:"C",1:"1B",2:"2B",3:"3B",4:"SS",5:"OF",6:"OF",7:"OF",
                10:"DH",11:"SP",12:"SP",13:"SP",14:"SP",15:"RP",16:"RP",
                17:"BE",18:"BE",19:"BE",20:"BE",21:"IL"}

    rosters_out       = []
    # For player scoring: collect ALL rostered players across all teams
    all_players_flat  = []   # list of dicts (includes tid, slot category, stats)
    team_player_groups = defaultdict(lambda: {
        "activeHitters":[], "activePitchers":[], "benchHitters":[], "benchPitchers":[]
    })

    if roster_data:
        for t in roster_data.get("teams", []):
            tid   = t["id"]
            tm    = team_map.get(tid, {})
            entries = t.get("roster", {}).get("entries", [])
            players = []

            for e in entries:
                ppe    = e.get("playerPoolEntry", {})
                player = ppe.get("player", {})
                slot   = e.get("lineupSlotId", 17)
                pos_id = player.get("defaultPositionId", 0)
                inj    = ppe.get("injuryStatus", player.get("injuryStatus", "ACTIVE"))

                # Season stats
                raw_stats = {}
                for stat_entry in ppe.get("stats", []):
                    if stat_entry.get("statSplitTypeId") == 0:
                        for sid, val in stat_entry.get("stats", {}).items():
                            label = STAT_MAP.get(str(sid), "")
                            if label:
                                raw_stats[label] = round(val, 3) if isinstance(val, float) else val

                on_bench = slot in BENCH_SLOTS
                on_il    = slot in IL_SLOTS
                is_active_hit   = slot in ACTIVE_HIT_SLOTS
                is_active_pitch = slot in ACTIVE_PITCH_SLOTS
                is_pitcher      = pos_id in PITCHER_POS_IDS

                player_dict = {
                    "name":      player.get("fullName", "Unknown"),
                    "position":  pos_map.get(pos_id, str(pos_id)),
                    "slot":      slot_map.get(slot, f"s{slot}"),
                    "injStatus": inj,
                    "onBench":   on_bench,
                    "onIL":      on_il,
                    "isPitcher": is_pitcher,
                    "proTeam":   player.get("proTeamId", 0),
                    "stats":     raw_stats,
                }
                players.append(player_dict)

                # Add to scoring groups (skip IL)
                if not on_il:
                    flat_entry = {"tid": tid, "stats": raw_stats, "isPitcher": is_pitcher}
                    all_players_flat.append(flat_entry)
                    if is_active_hit:
                        team_player_groups[tid]["activeHitters"].append(flat_entry)
                    elif is_active_pitch:
                        team_player_groups[tid]["activePitchers"].append(flat_entry)
                    elif on_bench:
                        if is_pitcher:
                            team_player_groups[tid]["benchPitchers"].append(flat_entry)
                        else:
                            team_player_groups[tid]["benchHitters"].append(flat_entry)

            # Sort: active first (hit then pitch), bench, IL
            slot_order = {"C":0,"1B":1,"2B":2,"3B":3,"SS":4,"OF":5,"DH":8,
                          "SP":9,"RP":10,"BE":11,"IL":12}
            players.sort(key=lambda p: slot_order.get(p["slot"], 99))

            rosters_out.append({
                "teamId":tid,"name":tm.get("name",""),"abbrev":tm.get("abbrev",""),
                "isMyTeam":tm.get("isMyTeam",False),"players":players,
            })

    save("rosters.json",{"season":CURRENT_SEASON,"teams":rosters_out,"updated":updated})

    # ── Power Rankings (composite: cat rank + roster quality) ─────────────────
    print("  → Computing power rankings …")
    all_tids       = [t["id"] for t in teams_raw]
    standings_map  = {s["id"]: s for s in standings}

    # --- Component 1: Category rank score ------------------------------------
    cat_ranks = {tid: {} for tid in all_tids}
    for cat, team_vals in cat_values.items():
        if not team_vals: continue
        lower_better = cat in LOWER_IS_BETTER
        sorted_teams = sorted(team_vals.items(), key=lambda x: x[1], reverse=not lower_better)
        for rank_idx, (tid, _) in enumerate(sorted_teams):
            cat_ranks[tid][cat] = rank_idx + 1

    # Raw rank score per team (sum of ranks, lower = better → invert for composite)
    rank_score_raw = {tid: sum(cat_ranks[tid].get(c, 6) for c in ALL_CATS) for tid in all_tids}
    # Invert so higher = better for composite weighting
    max_rs = max(rank_score_raw.values()) if rank_score_raw else 1
    min_rs = min(rank_score_raw.values()) if rank_score_raw else 0
    cat_score_norm = {
        tid: round((max_rs - v) / (max_rs - min_rs) * 100, 1) if max_rs != min_rs else 50.0
        for tid, v in rank_score_raw.items()
    }

    # H2H simulated record across all 14 cats (season stats)
    h2h = {tid: {"wins":0,"losses":0,"ties":0} for tid in all_tids}
    for i, tid_a in enumerate(all_tids):
        for tid_b in all_tids[i+1:]:
            a_wins = b_wins = ties = 0
            for cat in ALL_CATS:
                a_val = cat_values.get(cat, {}).get(tid_a)
                b_val = cat_values.get(cat, {}).get(tid_b)
                if a_val is None or b_val is None: continue
                if a_val == b_val: ties += 1
                elif (a_val < b_val) == (cat in LOWER_IS_BETTER): a_wins += 1
                else: b_wins += 1
            h2h[tid_a]["wins"]   += a_wins;  h2h[tid_a]["losses"] += b_wins
            h2h[tid_b]["wins"]   += b_wins;  h2h[tid_b]["losses"] += a_wins
            h2h[tid_a]["ties"]   += ties;    h2h[tid_b]["ties"]   += ties

    # --- Component 2–5: Roster quality z-scores ------------------------------
    # Compute z-scores for all non-IL players league-wide
    player_scores = compute_player_scores(all_players_flat)

    # Map player scores back to flat list entries
    for i, entry in enumerate(all_players_flat):
        entry["_hitScore"]   = player_scores[i]["hitScore"]
        entry["_pitchScore"] = player_scores[i]["pitchScore"]

    def group_score(players, use_hit=True):
        """Mean z-score for a group of players."""
        scores = [p["_hitScore"] if use_hit else p["_pitchScore"] for p in players]
        return sum(scores) / len(scores) if scores else 0.0

    raw_roster_scores = {}
    for tid in all_tids:
        grp = team_player_groups[tid]
        raw_roster_scores[tid] = {
            "starterHit":   group_score(grp["activeHitters"],   use_hit=True),
            "starterPitch": group_score(grp["activePitchers"],  use_hit=False),
            "benchHit":     group_score(grp["benchHitters"],    use_hit=True),
            "benchPitch":   group_score(grp["benchPitchers"],   use_hit=False),
            "hitDepth":     len(grp["activeHitters"]) + len(grp["benchHitters"]),
            "pitchDepth":   len(grp["activePitchers"]) + len(grp["benchPitchers"]),
        }

    # Normalize each roster component to 0–100 across all teams
    def norm_component(key):
        vals = [raw_roster_scores[tid][key] for tid in all_tids]
        normed = normalize_to_100(vals)
        return {tid: normed[i] for i, tid in enumerate(all_tids)}

    n_starter_hit   = norm_component("starterHit")
    n_starter_pitch = norm_component("starterPitch")
    n_bench_hit     = norm_component("benchHit")
    n_bench_pitch   = norm_component("benchPitch")

    # --- Composite score -----------------------------------------------------
    composite = {}
    for tid in all_tids:
        composite[tid] = round(
            W_CAT_RANK    * cat_score_norm[tid] +
            W_STARTER_HIT * n_starter_hit[tid] +
            W_STARTER_PIT * n_starter_pitch[tid] +
            W_BENCH_HIT   * n_bench_hit[tid] +
            W_BENCH_PIT   * n_bench_pitch[tid],
        1)

    # --- Build final list sorted by composite desc ---------------------------
    power = []
    for tid in all_tids:
        tm    = team_map.get(tid, {})
        s_rec = standings_map.get(tid, {})
        rs    = raw_roster_scores[tid]
        power.append({
            "id":         tid,
            "name":       tm.get("name", f"Team {tid}"),
            "abbrev":     tm.get("abbrev", ""),
            "isMyTeam":   tm.get("isMyTeam", False),
            "overallW":   s_rec.get("wins", 0),
            "overallL":   s_rec.get("losses", 0),
            "overallRank":s_rec.get("rank", 0),
            # Composite
            "composite":  composite[tid],
            # Component scores (0–100)
            "catScore":        cat_score_norm[tid],
            "starterHitScore": n_starter_hit[tid],
            "starterPitScore": n_starter_pitch[tid],
            "benchHitScore":   n_bench_hit[tid],
            "benchPitScore":   n_bench_pitch[tid],
            # Raw rank info
            "rankScore":  rank_score_raw[tid],
            "catRanks":   cat_ranks[tid],
            # H2H
            "h2hWins":    h2h[tid]["wins"],
            "h2hLosses":  h2h[tid]["losses"],
            "h2hTies":    h2h[tid]["ties"],
            # Roster depth counts
            "hitDepth":   rs["hitDepth"],
            "pitchDepth": rs["pitchDepth"],
        })

    power.sort(key=lambda x: -x["composite"])
    for i, p in enumerate(power):
        p["pwRank"]    = i + 1
        p["rankDelta"] = p["overallRank"] - p["pwRank"]

    save("power_rankings.json", {
        "week":     current_week,
        "cats":     ALL_CATS,
        "weights": {
            "catRank":    W_CAT_RANK,
            "starterHit": W_STARTER_HIT,
            "starterPit": W_STARTER_PIT,
            "benchHit":   W_BENCH_HIT,
            "benchPit":   W_BENCH_PIT,
        },
        "rankings": power,
        "updated":  updated,
    })

    # ── Meta ──────────────────────────────────────────────────────────────────
    settings = core.get("settings", {})
    save("meta.json", {
        "leagueName":    settings.get("name", "The League"),
        "season":        CURRENT_SEASON,
        "currentWeek":   current_week,
        "scoringPeriod": scoring_period,
        "teamCount":     len(teams_raw),
        "myTeam":        MY_TEAM,
        "updated":       updated,
    })

    print(f"\n  🏆  Current season done. Week {current_week}.")
    return team_map, standings


# ── HISTORICAL DATA ───────────────────────────────────────────────────────────
def fetch_history(current_team_map):
    print("\n━━ Historical Data ━━")
    updated = now_utc()

    all_season_standings = []
    all_matchups         = []
    h2h_records    = defaultdict(lambda: defaultdict(int))
    cat_by_season  = []

    for season in HISTORY_SEASONS:
        print(f"\n  Season {season} …")
        time.sleep(0.4)

        core = fetch(season, "mTeam", "mStandings", silent=True)
        if not core:
            print(f"    ⚠ Skipping {season}"); continue

        teams_raw = core.get("teams", [])
        members   = core.get("members", [])
        if not teams_raw:
            print(f"    ⚠ No teams in {season}"); continue

        team_map = build_team_map(teams_raw, members)

        season_teams = []
        for t in teams_raw:
            rec = t.get("record", {}).get("overall", {})
            tm  = team_map[t["id"]]
            season_teams.append({
                "name":          tm["name"],
                "abbrev":        tm["abbrev"],
                "owners":        tm["owners"],
                "wins":          rec.get("wins", 0),
                "losses":        rec.get("losses", 0),
                "ties":          rec.get("ties", 0),
                "pointsFor":     round(rec.get("pointsFor", 0), 1),
                "pointsAgainst": round(rec.get("pointsAgainst", 0), 1),
                "seed":          t.get("playoffSeed", 0),
                "playoffResult": t.get("rankCalculatedFinal", 0),
            })
        season_teams.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
        for i, s in enumerate(season_teams):
            s["rank"] = i + 1
        all_season_standings.append({"season":season,"teams":season_teams})

        time.sleep(0.4)
        sched_data = fetch(season, "mMatchup", "mMatchupScore", silent=True)
        if not sched_data:
            print(f"    ⚠ No schedule for {season}"); continue

        schedule = sched_data.get("schedule", [])
        print(f"    → {len(schedule)} matchups")
        cat_totals = defaultdict(lambda: defaultdict(lambda: {"W":0,"L":0,"T":0}))

        for m in schedule:
            mp_id   = m.get("matchupPeriodId", 0)
            home_r  = m.get("home", {})
            away_r  = m.get("away", {})
            h_tid   = home_r.get("teamId")
            a_tid   = away_r.get("teamId")
            if h_tid is None or a_tid is None: continue

            h_name = team_map.get(h_tid, {}).get("name", f"T{h_tid}")
            a_name = team_map.get(a_tid, {}).get("name", f"T{a_tid}")
            h_cum  = home_r.get("cumulativeScore", {})
            a_cum  = away_r.get("cumulativeScore", {})

            winner_raw  = m.get("winner", "UNDECIDED")
            winner_name = h_name if winner_raw=="HOME" else a_name if winner_raw=="AWAY" else "UNDECIDED"

            all_matchups.append({
                "season":season,"week":mp_id,
                "home":h_name,"away":a_name,
                "homeCatW":h_cum.get("wins",0),"awayCatW":a_cum.get("wins",0),
                "winner":winner_name,
            })

            if winner_raw in ("HOME","AWAY"):
                h2h_records[h_name][a_name] += (1 if winner_raw=="HOME" else 0)
                h2h_records[a_name][h_name] += (1 if winner_raw=="AWAY" else 0)
                h2h_records[h_name][f"__loss__{a_name}"] += (1 if winner_raw=="AWAY" else 0)
                h2h_records[a_name][f"__loss__{h_name}"] += (1 if winner_raw=="HOME" else 0)

            for stat_id, info in h_cum.get("scoreByStat", {}).items():
                label  = STAT_MAP.get(stat_id, f"s{stat_id}")
                result = info.get("result", "")
                if result == "WIN":
                    cat_totals[h_tid][label]["W"] += 1; cat_totals[a_tid][label]["L"] += 1
                elif result == "LOSS":
                    cat_totals[h_tid][label]["L"] += 1; cat_totals[a_tid][label]["W"] += 1
                elif result == "TIE":
                    cat_totals[h_tid][label]["T"] += 1; cat_totals[a_tid][label]["T"] += 1

        season_cat_teams = []
        for t in teams_raw:
            tid = t["id"]
            tm  = team_map[tid]
            season_cat_teams.append({"name":tm["name"],"catRecord":dict(cat_totals[tid])})
        cat_by_season.append({"season":season,"teams":season_cat_teams})

    # H2H dedup
    teams_seen = set()
    h2h_formatted = []
    seen_pairs = set()
    for team_a, opponents in h2h_records.items():
        if team_a.startswith("__"): continue
        teams_seen.add(team_a)
        for key, wins in opponents.items():
            if key.startswith("__loss__"): continue
            team_b = key
            teams_seen.add(team_b)
            pair = tuple(sorted([team_a, team_b]))
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                losses = h2h_records[team_a].get(f"__loss__{team_b}", 0)
                h2h_formatted.append({"teamA":team_a,"teamB":team_b,"aWins":wins,"aLosses":losses})

    save("history_standings.json",{"seasons":all_season_standings,"updated":updated})
    save("history_matchups.json", {"matchups":all_matchups,"updated":updated})
    save("history_h2h.json",      {"records":h2h_formatted,"teams":sorted(teams_seen),"updated":updated})
    save("history_cats.json",     {"seasons":cat_by_season,"cats":ALL_CATS,"updated":updated})
    print("\n  🏆  History done.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄  ESPN Fetcher | League {LEAGUE_ID} | Season {CURRENT_SEASON}")
    team_map, standings = fetch_current_season()
    fetch_history(team_map)
    print("\n✅  All data written to data/")
