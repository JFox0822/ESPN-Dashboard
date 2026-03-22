#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Full Data Fetcher
Uses the espn-api package to reliably pull live + historical data.
Runs daily via GitHub Actions.
"""

import json
import os
import sys
import time
import math
from datetime import datetime, timezone
from collections import defaultdict

try:
    from espn_api.baseball import League
except ImportError:
    print("Installing espn-api …")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "espn-api", "-q"])
    from espn_api.baseball import League

# ── Config ────────────────────────────────────────────────────────────────────
LEAGUE_ID       = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
CURRENT_SEASON  = 2026
MY_TEAM         = "Jacob"
HISTORY_SEASONS = list(range(2019, 2026))

HIT_CATS   = ["R","RBI","HR","SB","K","AVG","OPS"]
PITCH_CATS = ["IP","H","K","QS","ERA","WHIP","SV"]
ALL_CATS   = HIT_CATS + PITCH_CATS
LOWER_IS_BETTER = {"ERA","WHIP","H"}

# Lineup slot classification (espn-api slot IDs)
ACTIVE_HIT_SLOTS   = {0,1,2,3,4,5,6,7,10}
ACTIVE_PITCH_SLOTS = {11,12,13,14,15,16}
BENCH_SLOTS        = {17,18,19,20}
IL_SLOTS           = {21}
PITCHER_POS_IDS    = {1,11,14}   # SP, RP, P

HIT_SCORE_STATS   = ["R","RBI","HR","SB","AVG","OPS"]
PITCH_SCORE_STATS = {
    "IP":+1,"K":+1,"QS":+1,"SV":+1,"HLD":+1,
    "ERA":-1,"WHIP":-1,"H":-1,
}

W_CAT_RANK    = 0.30
W_STARTER_HIT = 0.25
W_STARTER_PIT = 0.25
W_BENCH_HIT   = 0.10
W_BENCH_PIT   = 0.10

# ── Helpers ───────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

def mean_std(values):
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0, 1.0
    m = sum(vals) / len(vals)
    var = sum((v-m)**2 for v in vals) / len(vals)
    return m, math.sqrt(var) if var > 0 else 1.0

def compute_z_scores(all_players):
    hit_vals   = {s:[] for s in HIT_SCORE_STATS}
    pitch_vals = {s:[] for s in PITCH_SCORE_STATS}
    for p in all_players:
        stats = p.get("stats", {})
        for s in HIT_SCORE_STATS:
            if stats.get(s) is not None: hit_vals[s].append(stats[s])
        for s in PITCH_SCORE_STATS:
            if stats.get(s) is not None: pitch_vals[s].append(stats[s])
    hn = {s: mean_std(hit_vals[s])   for s in HIT_SCORE_STATS}
    pn = {s: mean_std(pitch_vals[s]) for s in PITCH_SCORE_STATS}
    scores = []
    for p in all_players:
        stats = p.get("stats", {})
        h_zs = [(stats[s]-hn[s][0])/hn[s][1] for s in HIT_SCORE_STATS if stats.get(s) is not None]
        p_zs = [d*(stats[s]-pn[s][0])/pn[s][1] for s,d in PITCH_SCORE_STATS.items() if stats.get(s) is not None]
        scores.append({
            "hitScore":   sum(h_zs)/len(h_zs) if h_zs else 0.0,
            "pitchScore": sum(p_zs)/len(p_zs) if p_zs else 0.0,
        })
    return scores

def normalize_to_100(values):
    mn, mx = min(values), max(values)
    if mx == mn: return [50.0]*len(values)
    return [round((v-mn)/(mx-mn)*100, 1) for v in values]

def safe_stat(player, stat_name):
    """Pull a stat from espn-api player stats dict."""
    try:
        stats = player.stats or {}
        # espn-api stores season stats under key '00' or '001' etc.
        for key in ["00", "001", "0"]:
            if key in stats:
                return stats[key].get("total", {}).get(stat_name)
        # fallback: search all keys
        for v in stats.values():
            if isinstance(v, dict) and "total" in v:
                val = v["total"].get(stat_name)
                if val is not None:
                    return val
    except Exception:
        pass
    return None

def get_team_cat_stats(league, team):
    """Extract season cumulative category stats for a team."""
    result = {}
    try:
        for cat in ALL_CATS:
            val = None
            # Try team.stats dict first
            if hasattr(team, 'stats') and team.stats:
                for k, v in team.stats.items():
                    if isinstance(v, dict):
                        if cat in v:
                            val = v[cat]; break
                        total = v.get("total", {})
                        if cat in total:
                            val = total[cat]; break
            if val is not None:
                result[cat] = round(float(val), 3) if isinstance(val, float) else val
    except Exception as e:
        print(f"    ⚠ cat stats error for {getattr(team,'team_name','?')}: {e}")
    return result


# ── CURRENT SEASON ────────────────────────────────────────────────────────────
def fetch_current_season():
    print(f"\n━━ Current Season {CURRENT_SEASON} ━━")
    updated = now_utc()

    print("  → Connecting via espn-api …")
    try:
        league = League(league_id=LEAGUE_ID, year=CURRENT_SEASON)
    except Exception as e:
        print(f"  ❌ Failed to load league: {e}")
        sys.exit(1)

    current_week = league.current_week
    print(f"  → {len(league.teams)} teams | week {current_week}")

    # ── Build team info ───────────────────────────────────────────────────────
    def is_my_team(team):
        name = (team.team_name or "").lower()
        owners = " ".join(o.get("firstName","")+" "+o.get("lastName","")
                          for o in (team.owners or [])).lower()
        return MY_TEAM.lower() in name or MY_TEAM.lower() in owners

    team_info = {}
    for t in league.teams:
        team_info[t.team_id] = {
            "id":       t.team_id,
            "name":     t.team_name,
            "abbrev":   t.team_abbrev,
            "logo":     getattr(t, "logo_url", ""),
            "isMyTeam": is_my_team(t),
        }

    # ── Standings ─────────────────────────────────────────────────────────────
    standings = []
    for t in league.teams:
        tm   = team_info[t.team_id]
        wins = getattr(t, "wins", 0)
        loss = getattr(t, "losses", 0)
        ties = getattr(t, "ties", 0)
        pf   = round(getattr(t, "points_for", 0), 1)
        pa   = round(getattr(t, "points_against", 0), 1)
        streak_len  = getattr(t, "streak_length", 0)
        streak_type = getattr(t, "streak_type", "")
        standings.append({
            **tm,
            "wins":wins,"losses":loss,"ties":ties,
            "pointsFor":pf,"pointsAgainst":pa,
            "streak":streak_len,"streakType":streak_type,
            "seed":getattr(t,"playoff_pct",0),
        })
    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i+1
    save("standings.json", {"week":current_week,"standings":standings,"updated":updated})

    # ── Matchups ──────────────────────────────────────────────────────────────
    matchups_out = []
    try:
        box_scores = league.box_scores(current_week)
        for bs in box_scores:
            def side(team, score_obj):
                if team is None:
                    return {"teamId":None,"team":"BYE","abbrev":"BYE","catWins":0,"catLoss":0,"categories":{},"isMyTeam":False}
                tm = team_info.get(team.team_id, {})
                cat_wins = getattr(score_obj,"wins",0) if score_obj else 0
                cat_loss = getattr(score_obj,"losses",0) if score_obj else 0
                # Try to pull category breakdown
                cats = {}
                if score_obj and hasattr(score_obj, "scoring_settings"):
                    for cat, val in (score_obj.scoring_settings or {}).items():
                        cats[cat] = {"value": round(float(val),3) if val else 0, "result":""}
                return {
                    "teamId":   team.team_id,
                    "team":     team.team_name,
                    "abbrev":   team.team_abbrev,
                    "catWins":  cat_wins,
                    "catLoss":  cat_loss,
                    "categories": cats,
                    "isMyTeam": tm.get("isMyTeam", False),
                }

            home = side(bs.home_team,  getattr(bs,"home_score",None))
            away = side(bs.away_team,  getattr(bs,"away_score",None))
            leader = home["team"] if home["catWins"]>away["catWins"] else \
                     away["team"] if away["catWins"]>home["catWins"] else "Tied"
            winner = "UNDECIDED"
            if hasattr(bs, "winner"):
                w = bs.winner
                if w == "home": winner = "HOME"
                elif w == "away": winner = "AWAY"
            matchups_out.append({"home":home,"away":away,"leader":leader,"winner":winner})
    except Exception as e:
        print(f"  ⚠ Matchup fetch error: {e}")

    save("matchups.json",{"week":current_week,"matchups":matchups_out,"updated":updated})

    # ── Team Stats ────────────────────────────────────────────────────────────
    team_stats = []
    cat_values = defaultdict(dict)

    for t in league.teams:
        tm  = team_info[t.team_id]
        stats = get_team_cat_stats(league, t)
        for cat, val in stats.items():
            cat_values[cat][t.team_id] = val
        wins = getattr(t,"wins",0); loss = getattr(t,"losses",0)
        team_stats.append({**tm,"wins":wins,"losses":loss,"stats":stats})

    team_stats.sort(key=lambda x:(-x["wins"],x["losses"]))
    save("team_stats.json",{"season":CURRENT_SEASON,"teams":team_stats,"updated":updated})

    # ── Rosters + Player z-score scoring ──────────────────────────────────────
    print("  → Fetching rosters …")
    rosters_out      = []
    all_players_flat = []
    team_player_groups = defaultdict(lambda:{
        "activeHitters":[],"activePitchers":[],"benchHitters":[],"benchPitchers":[]
    })

    pos_map = {
        "C":"C","1B":"1B","2B":"2B","3B":"3B","SS":"SS",
        "OF":"OF","DH":"DH","SP":"SP","RP":"RP","P":"P","BE":"BE","IL":"IL",
    }
    slot_label = {
        0:"C",1:"1B",2:"2B",3:"3B",4:"SS",5:"OF",6:"OF",7:"OF",
        10:"DH",11:"SP",12:"SP",13:"SP",14:"SP",15:"RP",16:"RP",
        17:"BE",18:"BE",19:"BE",20:"BE",21:"IL",
    }

    for t in league.teams:
        tid = t.team_id
        tm  = team_info[tid]
        players_out = []

        roster = getattr(t, "roster", [])
        for player in roster:
            slot    = getattr(player, "lineupSlot", "BE")
            slot_id = getattr(player, "slot_position_id", 17)
            pos     = getattr(player, "position", "")
            inj     = getattr(player, "injuryStatus", "ACTIVE")
            name    = getattr(player, "name", "Unknown")

            # Pull season stats
            raw_stats = {}
            for cat in ALL_CATS + ["HLD"]:
                v = safe_stat(player, cat)
                if v is not None:
                    raw_stats[cat] = round(float(v),3) if isinstance(v,float) else v

            on_bench = slot_id in BENCH_SLOTS
            on_il    = slot_id in IL_SLOTS
            is_active_hit   = slot_id in ACTIVE_HIT_SLOTS
            is_active_pitch = slot_id in ACTIVE_PITCH_SLOTS
            is_pitcher = pos in ("SP","RP","P") or slot_id in ACTIVE_PITCH_SLOTS

            pd = {
                "name":name,"position":pos,
                "slot":slot_label.get(slot_id, str(slot)),
                "injStatus":inj,"onBench":on_bench,"onIL":on_il,
                "isPitcher":is_pitcher,"stats":raw_stats,
            }
            players_out.append(pd)

            if not on_il:
                flat = {"tid":tid,"stats":raw_stats,"isPitcher":is_pitcher}
                all_players_flat.append(flat)
                if is_active_hit:
                    team_player_groups[tid]["activeHitters"].append(flat)
                elif is_active_pitch:
                    team_player_groups[tid]["activePitchers"].append(flat)
                elif on_bench:
                    if is_pitcher:
                        team_player_groups[tid]["benchPitchers"].append(flat)
                    else:
                        team_player_groups[tid]["benchHitters"].append(flat)

        # Sort by slot
        slot_order = {"C":0,"1B":1,"2B":2,"3B":3,"SS":4,"OF":5,"DH":8,"SP":9,"RP":10,"BE":11,"IL":12}
        players_out.sort(key=lambda p: slot_order.get(p["slot"],99))

        rosters_out.append({
            "teamId":tid,"name":tm["name"],"abbrev":tm["abbrev"],
            "isMyTeam":tm["isMyTeam"],"players":players_out,
        })

    save("rosters.json",{"season":CURRENT_SEASON,"teams":rosters_out,"updated":updated})

    # ── Power Rankings ────────────────────────────────────────────────────────
    print("  → Computing power rankings …")
    all_tids      = [t.team_id for t in league.teams]
    standings_map = {s["id"]:s for s in standings}

    # Component 1: category rank score
    cat_ranks = {tid:{} for tid in all_tids}
    for cat, team_vals in cat_values.items():
        if not team_vals: continue
        lower = cat in LOWER_IS_BETTER
        srt = sorted(team_vals.items(), key=lambda x:x[1], reverse=not lower)
        for ri,(tid,_) in enumerate(srt):
            cat_ranks[tid][cat] = ri+1

    rank_score_raw = {tid: sum(cat_ranks[tid].get(c,6) for c in ALL_CATS) for tid in all_tids}
    max_rs = max(rank_score_raw.values()) if rank_score_raw else 1
    min_rs = min(rank_score_raw.values()) if rank_score_raw else 0
    cat_score_norm = {
        tid: round((max_rs-v)/(max_rs-min_rs)*100,1) if max_rs!=min_rs else 50.0
        for tid,v in rank_score_raw.items()
    }

    # H2H simulated record
    h2h = {tid:{"wins":0,"losses":0,"ties":0} for tid in all_tids}
    for i,tid_a in enumerate(all_tids):
        for tid_b in all_tids[i+1:]:
            aw=bw=t=0
            for cat in ALL_CATS:
                av=cat_values.get(cat,{}).get(tid_a)
                bv=cat_values.get(cat,{}).get(tid_b)
                if av is None or bv is None: continue
                if av==bv: t+=1
                elif (av<bv)==(cat in LOWER_IS_BETTER): aw+=1
                else: bw+=1
            h2h[tid_a]["wins"]+=aw; h2h[tid_a]["losses"]+=bw; h2h[tid_a]["ties"]+=t
            h2h[tid_b]["wins"]+=bw; h2h[tid_b]["losses"]+=aw; h2h[tid_b]["ties"]+=t

    # Component 2-5: player z-scores
    pscores = compute_z_scores(all_players_flat)
    for i,entry in enumerate(all_players_flat):
        entry["_hit"]=pscores[i]["hitScore"]; entry["_pit"]=pscores[i]["pitchScore"]

    def grp_score(players, hit=True):
        sc = [p["_hit"] if hit else p["_pit"] for p in players]
        return sum(sc)/len(sc) if sc else 0.0

    raw_rs = {}
    for tid in all_tids:
        g = team_player_groups[tid]
        raw_rs[tid] = {
            "starterHit":   grp_score(g["activeHitters"],True),
            "starterPitch": grp_score(g["activePitchers"],False),
            "benchHit":     grp_score(g["benchHitters"],True),
            "benchPitch":   grp_score(g["benchPitchers"],False),
            "hitDepth":     len(g["activeHitters"])+len(g["benchHitters"]),
            "pitchDepth":   len(g["activePitchers"])+len(g["benchPitchers"]),
        }

    def norm_comp(key):
        vals = [raw_rs[tid][key] for tid in all_tids]
        nv   = normalize_to_100(vals)
        return {tid:nv[i] for i,tid in enumerate(all_tids)}

    n_sh = norm_comp("starterHit"); n_sp = norm_comp("starterPitch")
    n_bh = norm_comp("benchHit");   n_bp = norm_comp("benchPitch")

    composite = {
        tid: round(
            W_CAT_RANK*cat_score_norm[tid] + W_STARTER_HIT*n_sh[tid] +
            W_STARTER_PIT*n_sp[tid] + W_BENCH_HIT*n_bh[tid] + W_BENCH_PIT*n_bp[tid], 1)
        for tid in all_tids
    }

    power = []
    for tid in all_tids:
        tm   = team_info[tid]
        sr   = standings_map.get(tid,{})
        rs   = raw_rs[tid]
        power.append({
            "id":tid,"name":tm["name"],"abbrev":tm["abbrev"],"isMyTeam":tm["isMyTeam"],
            "overallW":sr.get("wins",0),"overallL":sr.get("losses",0),"overallRank":sr.get("rank",0),
            "composite":composite[tid],
            "catScore":       cat_score_norm[tid],
            "starterHitScore":n_sh[tid],"starterPitScore":n_sp[tid],
            "benchHitScore":  n_bh[tid],"benchPitScore":  n_bp[tid],
            "rankScore":rank_score_raw[tid],"catRanks":cat_ranks[tid],
            "h2hWins":h2h[tid]["wins"],"h2hLosses":h2h[tid]["losses"],"h2hTies":h2h[tid]["ties"],
            "hitDepth":rs["hitDepth"],"pitchDepth":rs["pitchDepth"],
        })

    power.sort(key=lambda x: -x["composite"])
    for i,p in enumerate(power):
        p["pwRank"]=i+1; p["rankDelta"]=p["overallRank"]-p["pwRank"]

    save("power_rankings.json",{
        "week":current_week,"cats":ALL_CATS,
        "weights":{"catRank":W_CAT_RANK,"starterHit":W_STARTER_HIT,"starterPit":W_STARTER_PIT,
                   "benchHit":W_BENCH_HIT,"benchPit":W_BENCH_PIT},
        "rankings":power,"updated":updated,
    })

    # ── Meta ──────────────────────────────────────────────────────────────────
    save("meta.json",{
        "leagueName": getattr(league.settings,"league_name","The League"),
        "season":CURRENT_SEASON,"currentWeek":current_week,
        "teamCount":len(league.teams),"myTeam":MY_TEAM,"updated":updated,
    })

    print(f"\n  🏆  Current season done. Week {current_week}.")
    return league


# ── HISTORICAL DATA ───────────────────────────────────────────────────────────
def fetch_history():
    print("\n━━ Historical Data ━━")
    updated = now_utc()

    all_standings = []
    all_matchups  = []
    h2h_wins      = defaultdict(lambda: defaultdict(int))
    h2h_losses    = defaultdict(lambda: defaultdict(int))
    cat_by_season = []

    for season in HISTORY_SEASONS:
        print(f"\n  Season {season} …")
        time.sleep(1.0)
        try:
            league = League(league_id=LEAGUE_ID, year=season)
        except Exception as e:
            print(f"    ⚠ Could not load {season}: {e}"); continue

        teams = league.teams
        if not teams:
            print(f"    ⚠ No teams"); continue
        print(f"    → {len(teams)} teams")

        # Standings
        season_teams = []
        for t in teams:
            season_teams.append({
                "name":    t.team_name,
                "abbrev":  t.team_abbrev,
                "wins":    getattr(t,"wins",0),
                "losses":  getattr(t,"losses",0),
                "ties":    getattr(t,"ties",0),
                "pointsFor":   round(getattr(t,"points_for",0),1),
                "pointsAgainst": round(getattr(t,"points_against",0),1),
                "seed":    getattr(t,"final_standing",0),
                "playoffResult": getattr(t,"final_standing",0),
            })
        season_teams.sort(key=lambda x:(-x["wins"],x["losses"],-x["pointsFor"]))
        for i,s in enumerate(season_teams): s["rank"]=i+1
        all_standings.append({"season":season,"teams":season_teams})

        # Matchups
        time.sleep(0.5)
        cat_totals = defaultdict(lambda: defaultdict(lambda:{"W":0,"L":0,"T":0}))

        try:
            total_weeks = getattr(league.settings,"reg_season_count", 22)
            for week in range(1, total_weeks+1):
                try:
                    boxes = league.box_scores(week)
                except Exception:
                    break
                for bs in boxes:
                    ht = bs.home_team; at = bs.away_team
                    if ht is None or at is None: continue
                    hname = ht.team_name; aname = at.team_name
                    hscore = getattr(bs,"home_score",None)
                    ascore = getattr(bs,"away_score",None)
                    hcw = getattr(hscore,"wins",0) if hasattr(hscore,"wins") else 0
                    acw = getattr(ascore,"wins",0) if hasattr(ascore,"wins") else 0

                    winner_raw = getattr(bs,"winner","UNDECIDED")
                    if winner_raw=="home":   wname=hname
                    elif winner_raw=="away": wname=aname
                    else:                    wname="UNDECIDED"

                    all_matchups.append({
                        "season":season,"week":week,
                        "home":hname,"away":aname,
                        "homeCatW":hcw,"awayCatW":acw,"winner":wname,
                    })

                    if wname not in ("UNDECIDED",""):
                        h2h_wins[hname][aname]   += (1 if wname==hname else 0)
                        h2h_wins[aname][hname]   += (1 if wname==aname else 0)
                        h2h_losses[hname][aname] += (1 if wname==aname else 0)
                        h2h_losses[aname][hname] += (1 if wname==hname else 0)

                    # Category breakdown if available
                    for bside,bname,btid in [(hscore,hname,ht.team_id),(ascore,aname,at.team_id)]:
                        if bside is None: continue
                        for cat in ALL_CATS:
                            result = ""
                            if hasattr(bside,"scoring_settings"):
                                # try to infer win/loss from cat values
                                pass
                            # Use wins/losses count proxy
                            # (detailed per-cat history requires deeper API calls; skip for now)

        except Exception as e:
            print(f"    ⚠ Matchup error for {season}: {e}")

        # Cat by season (use team season stats instead)
        season_cat_teams = []
        for t in teams:
            cat_rec = {}
            for cat in ALL_CATS:
                cat_rec[cat] = {"W":0,"L":0,"T":0}
            season_cat_teams.append({"name":t.team_name,"catRecord":cat_rec})
        cat_by_season.append({"season":season,"teams":season_cat_teams})

        time.sleep(0.5)

    # H2H formatted
    all_team_names = set()
    for tn in h2h_wins: all_team_names.add(tn)
    for tn in h2h_losses: all_team_names.add(tn)

    seen_pairs = set()
    h2h_out = []
    for ta in sorted(all_team_names):
        for tb in sorted(all_team_names):
            if ta==tb: continue
            pair = tuple(sorted([ta,tb]))
            if pair in seen_pairs: continue
            seen_pairs.add(pair)
            h2h_out.append({
                "teamA":ta,"teamB":tb,
                "aWins":h2h_wins[ta][tb],"aLosses":h2h_losses[ta][tb],
            })

    save("history_standings.json",{"seasons":all_standings,"updated":updated})
    save("history_matchups.json", {"matchups":all_matchups,"updated":updated})
    save("history_h2h.json",      {"records":h2h_out,"teams":sorted(all_team_names),"updated":updated})
    save("history_cats.json",     {"seasons":cat_by_season,"cats":ALL_CATS,"updated":updated})
    print("\n  🏆  History done.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄  ESPN Fetcher | League {LEAGUE_ID} | Season {CURRENT_SEASON}")
    fetch_current_season()
    fetch_history()
    print("\n✅  All data written to data/")
