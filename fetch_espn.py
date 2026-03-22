#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Full Data Fetcher
- Projection/actual blend (heavy proj early, 100% actuals by week 12)
- Historical data via league.schedule (efficient: 1 call/season)
- Legacy scores + team resumes
"""

import json, os, sys, time, math
from datetime import datetime, timezone
from collections import defaultdict

try:
    from espn_api.baseball import League
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "espn-api", "-q"])
    from espn_api.baseball import League

# ── Config ────────────────────────────────────────────────────────────────────
LEAGUE_ID       = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
CURRENT_SEASON  = 2026
MY_TEAM         = "Jacob"
HISTORY_SEASONS = list(range(2019, 2026))
TOTAL_TEAMS     = 12
PLAYOFF_SPOTS   = 6
FULL_ACTUAL_WEEK = 12   # week 12+ = 100% actuals; week 1 = 90% projected

HIT_CATS   = ["R","RBI","HR","SB","K","AVG","OPS"]
PITCH_CATS = ["IP","H","K","QS","ERA","WHIP","SV"]
ALL_CATS   = HIT_CATS + PITCH_CATS
LOWER_IS_BETTER = {"ERA","WHIP","H"}

ACTIVE_HIT_SLOTS   = {0,1,2,3,4,5,6,7,10}
ACTIVE_PITCH_SLOTS = {11,12,13,14,15,16}
BENCH_SLOTS        = {17,18,19,20}
IL_SLOTS           = {21}
PITCHER_POS        = {"SP","RP","P"}

HIT_SCORE_STATS   = ["R","RBI","HR","SB","AVG","OPS"]
PITCH_SCORE_STATS = {"IP":+1,"K":+1,"QS":+1,"SV":+1,"HLD":+1,"ERA":-1,"WHIP":-1,"H":-1}

W_CAT_RANK    = 0.30
W_STARTER_HIT = 0.25
W_STARTER_PIT = 0.25
W_BENCH_HIT   = 0.10
W_BENCH_PIT   = 0.10

LEGACY_CHAMP         = 150
LEGACY_RUNNER_UP     = 75
LEGACY_PLAYOFF       = 20
LEGACY_WIN_SEASON    = 12
LEGACY_CONSISTENCY   = 50   # max bonus
LEGACY_CAT_BALANCE   = 40   # max bonus

SLOT_LABEL = {0:"C",1:"1B",2:"2B",3:"3B",4:"SS",5:"OF",6:"OF",7:"OF",
              10:"DH",11:"SP",12:"SP",13:"SP",14:"SP",15:"RP",16:"RP",
              17:"BE",18:"BE",19:"BE",20:"BE",21:"IL"}
SLOT_ORDER = {"C":0,"1B":1,"2B":2,"3B":3,"SS":4,"OF":5,"DH":8,"SP":9,"RP":10,"BE":11,"IL":12}

# ── Projection blend ──────────────────────────────────────────────────────────
def proj_weight(week):
    """90% proj at week 1, linearly fades to 0% by FULL_ACTUAL_WEEK."""
    if week >= FULL_ACTUAL_WEEK:
        return 0.0
    return round(0.9 * (1.0 - (week - 1) / max(FULL_ACTUAL_WEEK - 1, 1)), 3)

def blend(actual, projected, pw):
    """Blend actual and projected values."""
    if actual is not None and projected is not None:
        return actual * (1 - pw) + projected * pw
    return actual if actual is not None else projected

# ── Stat extraction ───────────────────────────────────────────────────────────
# ── Stat key inspector (runs once, prints to Action log) ─────────────────────
_stat_keys_dumped = False

def dump_stat_keys(player):
    """Print available stat keys for one player to help debug ESPN API structure."""
    global _stat_keys_dumped
    if _stat_keys_dumped:
        return
    _stat_keys_dumped = True
    stats = getattr(player, "stats", {}) or {}
    print(f"  [DEBUG] Stat keys for {getattr(player,'name','?')}: {list(stats.keys())[:20]}")
    for k, v in list(stats.items())[:4]:
        if isinstance(v, dict):
            inner = {ik: iv for ik, iv in list(v.items())[:6] if ik != "stats"}
            # Show first few stat values
            stat_sample = {}
            raw_stats = v.get("stats") or v.get("total") or {}
            if isinstance(raw_stats, dict):
                # Map a few known ESPN stat IDs to names
                id_map = {"20":"R","21":"RBI","5":"HR","23":"SB","2":"AVG",
                          "34":"IP","47":"ERA","53":"WHIP","41":"H_allowed","48":"K_pitch"}
                for sid, name in id_map.items():
                    if sid in raw_stats:
                        stat_sample[name] = raw_stats[sid]
            print(f"  [DEBUG]   key={k!r}: {inner} | sample_stats={stat_sample}")


def extract_stats_from_entry(entry):
    """Extract category stats from a single ESPN stat entry dict."""
    raw = {}
    if not isinstance(entry, dict):
        return raw

    # ESPN stores stats as a sub-dict keyed by stat ID strings
    # The stats sub-dict can be under "stats", "total", or at the top level
    stat_dict = entry.get("stats") or entry.get("total") or {}

    if isinstance(stat_dict, dict) and stat_dict:
        # Keys are ESPN stat IDs (strings like "20", "5", etc.)
        # Map them to our category names
        ESPN_STAT_IDS = {
            "20":"R", "21":"RBI", "5":"HR", "23":"SB", "27":"K",
            "2":"AVG", "17":"OPS", "34":"IP", "41":"H", "48":"K",
            "63":"QS", "47":"ERA", "53":"WHIP", "57":"SV", "83":"HLD",
        }
        for stat_id, cat_name in ESPN_STAT_IDS.items():
            v = stat_dict.get(stat_id)
            if v is not None:
                try:
                    fv = float(v)
                    if fv != 0:
                        raw[cat_name] = round(fv, 3)
                except (ValueError, TypeError):
                    pass
        if raw:
            return raw

    # Fallback: entry uses category name strings directly (espn-api sometimes does this)
    for cat in ALL_CATS + ["HLD"]:
        v = entry.get(cat)
        if v is not None:
            try:
                fv = float(v)
                if fv != 0:
                    raw[cat] = round(fv, 3)
            except (ValueError, TypeError):
                pass

    return raw


def get_stats(player, projected=False, debug=False):
    """
    Pull season stats from espn-api player.
    ESPN stat entry keys follow the pattern: {statSplitTypeId}{seasonId}
      Type 0 = actual totals,  Type 1 = projected totals
    Season IDs: 2026, 2025, etc.
    Combined key examples: "002026" (2026 actuals), "012026" (2026 projected),
                           "002025" (2025 actuals), "012025" (2025 projected)
    """
    raw = {}
    try:
        stats = getattr(player, "stats", {}) or {}
        if not stats:
            return raw

        if debug:
            dump_stat_keys(player)

        # Build ordered candidate keys based on what we want
        if projected:
            # Want: 2026 projections first, then 2025 projections, then any proj
            priority = ["012026", "012025", "12026", "12025", "10", "1"]
            # Also grab any key where statSplitTypeId == 1
            dynamic = [k for k, v in stats.items()
                       if isinstance(v, dict) and str(v.get("statSplitTypeId","")) == "1"
                       and k not in priority]
            candidate_keys = dynamic + priority
        else:
            # Want: 2026 actuals, then 2025 actuals
            priority = ["002026", "002025", "02026", "02025", "00", "0"]
            dynamic = [k for k, v in stats.items()
                       if isinstance(v, dict) and str(v.get("statSplitTypeId","")) == "0"
                       and k not in priority]
            candidate_keys = dynamic + priority

        for key in candidate_keys:
            if key not in stats:
                continue
            entry_raw = extract_stats_from_entry(stats[key])
            if entry_raw:
                return entry_raw

        # Last resort: find entry with most populated stats regardless of type
        best_raw = {}
        best_count = 0
        for k, v in stats.items():
            if not isinstance(v, dict):
                continue
            entry_raw = extract_stats_from_entry(v)
            if len(entry_raw) > best_count:
                best_count = len(entry_raw)
                best_raw = entry_raw
        if best_count >= 3:
            return best_raw

    except Exception as e:
        pass
    return raw


def get_prior_year_stats(player):
    """Pull 2025 actual stats as projection baseline when ESPN projections unavailable."""
    raw = {}
    try:
        stats = getattr(player, "stats", {}) or {}
        # Try 2025 actual keys explicitly
        for key in ["002025", "02025", "012025"]:
            if key in stats:
                entry_raw = extract_stats_from_entry(stats[key])
                if entry_raw:
                    return entry_raw
        # Dynamic scan for 2025
        for k, v in stats.items():
            if "2025" in str(k) and isinstance(v, dict):
                entry_raw = extract_stats_from_entry(v)
                if entry_raw:
                    return entry_raw
    except Exception:
        pass
    return raw


# ── Z-score / normalization helpers ───────────────────────────────────────────
def mean_std(values):
    vals = [v for v in values if v is not None]
    if len(vals) < 2: return 0.0, 1.0
    m = sum(vals) / len(vals)
    var = sum((v-m)**2 for v in vals) / len(vals)
    return m, math.sqrt(var) if var > 0 else 1.0

def compute_z_scores(all_players):
    hv = {s:[] for s in HIT_SCORE_STATS}
    pv = {s:[] for s in PITCH_SCORE_STATS}
    for p in all_players:
        s = p.get("stats", {})
        for stat in HIT_SCORE_STATS:
            if s.get(stat) is not None: hv[stat].append(s[stat])
        for stat in PITCH_SCORE_STATS:
            if s.get(stat) is not None: pv[stat].append(s[stat])
    hn = {s: mean_std(hv[s]) for s in HIT_SCORE_STATS}
    pn = {s: mean_std(pv[s]) for s in PITCH_SCORE_STATS}
    scores = []
    for p in all_players:
        s = p.get("stats", {})
        hz = [(s[stat]-hn[stat][0])/hn[stat][1] for stat in HIT_SCORE_STATS if s.get(stat) is not None]
        pz = [d*(s[stat]-pn[stat][0])/pn[stat][1] for stat,d in PITCH_SCORE_STATS.items() if s.get(stat) is not None]
        scores.append({
            "hitScore":   sum(hz)/len(hz) if hz else 0.0,
            "pitchScore": sum(pz)/len(pz) if pz else 0.0,
        })
    return scores

def normalize_to_100(values):
    mn, mx = min(values), max(values)
    if mx == mn: return [50.0]*len(values)
    return [round((v-mn)/(mx-mn)*100, 1) for v in values]

# ── Misc ──────────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

def is_my_team(team):
    name   = (team.team_name or "").lower()
    owners = " ".join(
        f"{o.get('firstName','')} {o.get('lastName','')}".strip()
        for o in (team.owners or [])
    ).lower()
    return MY_TEAM.lower() in name or MY_TEAM.lower() in owners

def team_base(t):
    return {
        "id":       t.team_id,
        "name":     t.team_name,
        "abbrev":   t.team_abbrev,
        "logo":     getattr(t, "logo_url", ""),
        "isMyTeam": is_my_team(t),
    }


# ── CURRENT SEASON ────────────────────────────────────────────────────────────
def fetch_current_season():
    print(f"\n━━ Current Season {CURRENT_SEASON} ━━")
    updated = now_utc()

    print("  → Connecting …")
    try:
        league = League(league_id=LEAGUE_ID, year=CURRENT_SEASON)
    except Exception as e:
        print(f"  ❌ {e}"); sys.exit(1)

    week   = league.current_week
    pw     = proj_weight(week)
    pct_p  = round(pw * 100)
    pct_a  = 100 - pct_p
    print(f"  → Week {week} | proj blend: {pct_p}% projected / {pct_a}% actual")

    team_info = {t.team_id: team_base(t) for t in league.teams}

    # ── Standings ─────────────────────────────────────────────────────────────
    standings = []
    for t in league.teams:
        tm = team_info[t.team_id]
        standings.append({
            **tm,
            "wins":   getattr(t,"wins",0),
            "losses": getattr(t,"losses",0),
            "ties":   getattr(t,"ties",0),
            "pointsFor":     round(getattr(t,"points_for",0),1),
            "pointsAgainst": round(getattr(t,"points_against",0),1),
            "streak":     getattr(t,"streak_length",0),
            "streakType": getattr(t,"streak_type",""),
        })
    standings.sort(key=lambda x:(-x["wins"],x["losses"],-x["pointsFor"]))
    for i,s in enumerate(standings): s["rank"] = i+1
    save("standings.json", {"week":week,"standings":standings,"updated":updated})

    # ── Matchups ──────────────────────────────────────────────────────────────
    matchups_out = []
    try:
        box_scores = league.box_scores(week)
        print(f"  → {len(box_scores)} matchups this week")
        for bs in box_scores:
          try:
            def parse_side(team, lineup):
                if team is None:
                    return {"teamId":None,"team":"BYE","abbrev":"","catWins":0,"catLoss":0,"categories":{},"isMyTeam":False}
                tm = team_info.get(team.team_id, {})

                # Count category wins from the box score lineup
                # espn-api stores per-stat results in bs.home_stats / bs.away_stats
                categories = {}
                cat_wins = cat_loss = cat_ties = 0

                # Try home_stats / away_stats dict (key = stat name, value = {value, result})
                stats_key = "home_stats" if lineup == "home" else "away_stats"
                stats_dict = getattr(bs, stats_key, {}) or {}
                for stat_name, stat_info in stats_dict.items():
                    if not isinstance(stat_info, dict): continue
                    val    = stat_info.get("value", stat_info.get("score", 0))
                    result = stat_info.get("result", "")
                    categories[stat_name] = {"value": round(float(val),3) if val else 0, "result": result}
                    if result == "WIN":   cat_wins  += 1
                    elif result == "LOSS": cat_loss  += 1
                    elif result == "TIE":  cat_ties  += 1

                # Fallback: try cumulative score object attributes
                if cat_wins == 0 and cat_loss == 0:
                    score_obj = getattr(bs, f"{'home' if lineup=='home' else 'away'}_score", None)
                    if score_obj:
                        cat_wins  = getattr(score_obj, "wins",   getattr(score_obj, "cat_wins",  0))
                        cat_loss  = getattr(score_obj, "losses", getattr(score_obj, "cat_losses",0))

                return {
                    "teamId":     team.team_id,
                    "team":       team.team_name,
                    "abbrev":     team.team_abbrev,
                    "catWins":    cat_wins,
                    "catLoss":    cat_loss,
                    "catTies":    cat_ties,
                    "categories": categories,
                    "isMyTeam":   tm.get("isMyTeam", False),
                }

            home = parse_side(bs.home_team, "home")
            away = parse_side(bs.away_team, "away")

            leader = home["team"] if home["catWins"] > away["catWins"] else \
                     away["team"] if away["catWins"] > home["catWins"] else "Tied"

            winner = getattr(bs, "winner", "UNDECIDED") or "UNDECIDED"
            if winner == "home":  winner = "HOME"
            elif winner == "away": winner = "AWAY"
            else: winner = "UNDECIDED"

            matchups_out.append({"home":home,"away":away,"leader":leader,"winner":winner})
          except Exception as be:
            print(f"  ⚠ Skipping one matchup: {be}")

        print(f"  → Matchup cat wins sample: {matchups_out[0]['home']['catWins'] if matchups_out else 'n/a'}")
    except Exception as e:
        import traceback
        print(f"  ⚠ Matchups error: {e}")
        traceback.print_exc()
    save("matchups.json",{"week":week,"matchups":matchups_out,"updated":updated})

    # ── Rosters + team stats (blended) ────────────────────────────────────────
    print(f"  → Rosters (blend: {pct_p}%P/{pct_a}%A) …")
    rosters_out      = []
    all_players_flat = []
    team_groups      = defaultdict(lambda:{"activeHitters":[],"activePitchers":[],"benchHitters":[],"benchPitchers":[]})
    cat_values       = defaultdict(dict)   # cat → {tid: blended_team_value}
    team_stats_out   = []

    for t in league.teams:
        tid  = t.team_id
        tm   = team_info[tid]
        players_out = []
        team_cat_accum = defaultdict(float)
        team_cat_count = defaultdict(int)

        for player in getattr(t,"roster",[]):
            # espn-api exposes lineupSlot as a string like "C","SP","BE","IL" etc.
            lineup_slot = getattr(player, "lineupSlot", None) or ""
            slot_id     = getattr(player, "slot_position_id", None)
            pos         = getattr(player, "position", "")
            inj         = getattr(player, "injuryStatus", "ACTIVE")
            name        = getattr(player, "name", "Unknown")
            stats       = blended_stats(player, pw)

            # Determine slot label — prefer lineupSlot string, fall back to ID map
            PITCHER_SLOTS = {"SP","RP","P"}
            BENCH_STR     = {"BE"}
            IL_STR        = {"IL","IL10","IL60"}

            if lineup_slot:
                slot_str = lineup_slot.upper().strip()
                # Normalize common variations
                if slot_str in ("BENCH","BN"): slot_str = "BE"
                if slot_str in ("DL","DL10","DL60","60IL","10IL"): slot_str = "IL"
            elif slot_id is not None:
                slot_str = SLOT_LABEL.get(slot_id, "BE")
            else:
                slot_str = "BE"

            on_bench = slot_str in BENCH_STR
            on_il    = slot_str in IL_STR
            is_active = not on_bench and not on_il
            is_pitcher = pos in PITCHER_POS or slot_str in PITCHER_SLOTS

            pd = {
                "name":     name,
                "position": pos,
                "slot":     slot_str,
                "injStatus":inj,
                "onBench":  on_bench,
                "onIL":     on_il,
                "isPitcher":is_pitcher,
                "stats":    stats,
            }
            players_out.append(pd)

            if not on_il:
                flat = {"tid":tid,"stats":stats,"isPitcher":is_pitcher}
                all_players_flat.append(flat)
                if is_active and not is_pitcher:
                    team_groups[tid]["activeHitters"].append(flat)
                    for cat,v in stats.items():
                        team_cat_accum[cat] += v
                        team_cat_count[cat] += 1
                elif is_active and is_pitcher:
                    team_groups[tid]["activePitchers"].append(flat)
                    for cat,v in stats.items():
                        team_cat_accum[cat] += v
                        team_cat_count[cat] += 1
                elif on_bench:
                    if is_pitcher: team_groups[tid]["benchPitchers"].append(flat)
                    else:          team_groups[tid]["benchHitters"].append(flat)

        players_out.sort(key=lambda p: SLOT_ORDER.get(p["slot"],99))
        rosters_out.append({
            "teamId":tid,"name":tm["name"],"abbrev":tm["abbrev"],
            "isMyTeam":tm["isMyTeam"],"players":players_out,
        })

        # Team-level blended cat values (summed from active starters)
        team_cats = {}
        for cat in ALL_CATS:
            if team_cat_count.get(cat,0) > 0:
                team_cats[cat] = round(team_cat_accum[cat], 3)
        for cat,v in team_cats.items():
            cat_values[cat][tid] = v

        rec = next((s for s in standings if s["id"]==tid),{})
        team_stats_out.append({**tm,"wins":rec.get("wins",0),"losses":rec.get("losses",0),"stats":team_cats})

    total   = _proj_hits[2]
    espn_p  = _proj_hits[0]
    prior_p = _proj_hits[1]
    no_p    = total - espn_p - prior_p
    print(f"  → Stat sources: {espn_p} ESPN projections, {prior_p} prior-year baselines, {no_p} actuals-only (of {total} players)")
    if espn_p == 0 and prior_p == 0:
        print("  ⚠ No projection/prior-year data found — power rankings will be equal until ESPN data available")
    save("rosters.json",{"season":CURRENT_SEASON,"teams":rosters_out,
                         "projBlend":{"projPct":pct_p,"actualPct":pct_a,"week":week},
                         "updated":updated})
    team_stats_out.sort(key=lambda x:(-x["wins"],x["losses"]))
    save("team_stats.json",{"season":CURRENT_SEASON,"teams":team_stats_out,"updated":updated})

    # ── Power Rankings ────────────────────────────────────────────────────────
    print("  → Power rankings …")
    all_tids     = [t.team_id for t in league.teams]
    standings_map = {s["id"]:s for s in standings}

    # Cat rank score
    cat_ranks = {tid:{} for tid in all_tids}
    for cat, tv in cat_values.items():
        if not tv: continue
        srt = sorted(tv.items(), key=lambda x:x[1], reverse=(cat not in LOWER_IS_BETTER))
        for ri,(tid,_) in enumerate(srt): cat_ranks[tid][cat] = ri+1

    rsr = {tid: sum(cat_ranks[tid].get(c,6) for c in ALL_CATS) for tid in all_tids}
    max_rs = max(rsr.values()) if rsr else 1
    min_rs = min(rsr.values()) if rsr else 0
    # If all equal (no stat data yet), spread scores by standings rank so teams differ
    if max_rs == min_rs:
        print("  ⚠ Cat values all equal — using standings rank as tiebreaker")
        for i, tid in enumerate(all_tids):
            s_rank = standings_map.get(tid, {}).get("rank", i+1)
            rsr[tid] = s_rank
        max_rs = max(rsr.values()); min_rs = min(rsr.values())
    cat_norm = {
        tid: round((max_rs-v)/(max_rs-min_rs)*100,1) if max_rs!=min_rs else 50.0
        for tid,v in rsr.items()
    }

    # H2H simulated record (14 cats)
    h2h = {tid:{"wins":0,"losses":0,"ties":0} for tid in all_tids}
    for i,ta in enumerate(all_tids):
        for tb in all_tids[i+1:]:
            aw=bw=ties=0
            for cat in ALL_CATS:
                av=cat_values.get(cat,{}).get(ta)
                bv=cat_values.get(cat,{}).get(tb)
                if av is None or bv is None: continue
                if av==bv: ties+=1
                elif (av<bv)==(cat in LOWER_IS_BETTER): aw+=1
                else: bw+=1
            h2h[ta]["wins"]+=aw; h2h[ta]["losses"]+=bw; h2h[ta]["ties"]+=ties
            h2h[tb]["wins"]+=bw; h2h[tb]["losses"]+=aw; h2h[tb]["ties"]+=ties

    # Player z-scores — store in flat list AND back in roster player dicts
    pscores = compute_z_scores(all_players_flat)
    for i,e in enumerate(all_players_flat):
        e["_hit"]=pscores[i]["hitScore"]; e["_pit"]=pscores[i]["pitchScore"]

    # Propagate z-scores + tiers back to roster player dicts
    flat_idx = 0
    for roster_team in rosters_out:
        for player in roster_team["players"]:
            if not player["onIL"] and flat_idx < len(pscores):
                hs = pscores[flat_idx]["hitScore"]
                ps = pscores[flat_idx]["pitchScore"]
                player["hitScore"]  = round(hs, 3)
                player["pitchScore"] = round(ps, 3)
                if player["isPitcher"]:
                    player["tier"] = "ELITE" if ps > 1.0 else "SOLID" if ps > 0.3 else ""
                else:
                    player["tier"] = "ELITE" if hs > 1.0 else "SOLID" if hs > 0.3 else ""
                flat_idx += 1

    def grp(players, hit=True):
        sc=[p["_hit"] if hit else p["_pit"] for p in players]
        return sum(sc)/len(sc) if sc else 0.0

    raw_rs = {}
    for tid in all_tids:
        g = team_groups[tid]
        raw_rs[tid] = {
            "starterHit":   grp(g["activeHitters"],True),
            "starterPitch": grp(g["activePitchers"],False),
            "benchHit":     grp(g["benchHitters"],True),
            "benchPitch":   grp(g["benchPitchers"],False),
            "hitDepth":     len(g["activeHitters"])+len(g["benchHitters"]),
            "pitchDepth":   len(g["activePitchers"])+len(g["benchPitchers"]),
        }

    def nc(key):
        vals = [raw_rs[tid][key] for tid in all_tids]
        # If all equal (no stat data yet), return 50.0 for everyone
        # — depth-based proxies created misleading rankings early in season
        all_eq = len(set(round(v,4) for v in vals)) <= 1
        if all_eq:
            return {tid: 50.0 for tid in all_tids}
        nv = normalize_to_100(vals)
        return {tid:nv[i] for i,tid in enumerate(all_tids)}

    n_sh=nc("starterHit"); n_sp=nc("starterPitch")
    n_bh=nc("benchHit");   n_bp=nc("benchPitch")

    composite = {
        tid: round(W_CAT_RANK*cat_norm[tid]+W_STARTER_HIT*n_sh[tid]+
                   W_STARTER_PIT*n_sp[tid]+W_BENCH_HIT*n_bh[tid]+W_BENCH_PIT*n_bp[tid],1)
        for tid in all_tids
    }

    power = []
    for tid in all_tids:
        tm = team_info[tid]; sr = standings_map.get(tid,{}); rs = raw_rs[tid]
        power.append({
            "id":tid,"name":tm["name"],"abbrev":tm["abbrev"],"isMyTeam":tm["isMyTeam"],
            "overallW":sr.get("wins",0),"overallL":sr.get("losses",0),"overallRank":sr.get("rank",0),
            "composite":composite[tid],
            "catScore":cat_norm[tid],
            "starterHitScore":n_sh[tid],"starterPitScore":n_sp[tid],
            "benchHitScore":n_bh[tid],"benchPitScore":n_bp[tid],
            "rankScore":rsr[tid],"catRanks":cat_ranks[tid],
            "h2hWins":h2h[tid]["wins"],"h2hLosses":h2h[tid]["losses"],"h2hTies":h2h[tid]["ties"],
            "hitDepth":rs["hitDepth"],"pitchDepth":rs["pitchDepth"],
        })

    power.sort(key=lambda x:-x["composite"])
    for i,p in enumerate(power):
        p["pwRank"]=i+1; p["rankDelta"]=p["overallRank"]-p["pwRank"]

    save("power_rankings.json",{
        "week":week,"cats":ALL_CATS,
        "projBlend":{"projPct":pct_p,"actualPct":pct_a},
        "weights":{"catRank":W_CAT_RANK,"starterHit":W_STARTER_HIT,"starterPit":W_STARTER_PIT,
                   "benchHit":W_BENCH_HIT,"benchPit":W_BENCH_PIT},
        "rankings":power,"updated":updated,
    })

    # ── Meta ──────────────────────────────────────────────────────────────────
    save("meta.json",{
        "leagueName": getattr(getattr(league,"settings",None),"league_name","The League"),
        "season":CURRENT_SEASON,"currentWeek":week,
        "teamCount":len(league.teams),"myTeam":MY_TEAM,"updated":updated,
    })
    print(f"\n  🏆  Current season done. Week {week}.")
    return league




# ── HISTORICAL DATA ───────────────────────────────────────────────────────────
def fetch_history():
    print("\n━━ Historical Data ━━")
    updated = now_utc()

    # Accumulators
    all_standings  = []
    all_matchups   = []
    h2h_wins       = defaultdict(lambda: defaultdict(int))
    h2h_losses     = defaultdict(lambda: defaultdict(int))
    cat_by_season  = []
    # For legacy/resume: per-owner aggregation
    # key = owner_key (first+last name, lowercased, no spaces)
    franchise_data = defaultdict(lambda:{
        "names":set(),"seasons":[],"championships":0,"runnerUps":0,
        "playoffApps":0,"winningSeasons":0,"allW":0,"allL":0,
        "catWins":defaultdict(int),"catLosses":defaultdict(int),
        "finishes":[],"streaks":{"curW":0,"curL":0,"maxW":0,"maxL":0},
    })

    def owner_key(team):
        owners = getattr(team,"owners",[]) or []
        if owners:
            o = owners[0]
            return f"{o.get('firstName','').lower()}{o.get('lastName','').lower()}"
        return team.team_name.lower().replace(" ","")

    for season in HISTORY_SEASONS:
        print(f"\n  Season {season} …")
        time.sleep(1.0)
        try:
            league = League(league_id=LEAGUE_ID, year=season)
        except Exception as e:
            print(f"    ⚠ Could not load {season}: {e}"); continue

        teams = league.teams
        if not teams: print(f"    ⚠ No teams"); continue
        print(f"    → {len(teams)} teams, {len(league.schedule)} matchups")

        # Build owner→team map for this season
        owner_map = {}   # owner_key → team object
        for t in teams:
            ok = owner_key(t)
            owner_map[ok] = t

        # Standings
        season_teams = []
        for t in teams:
            wins = getattr(t,"wins",0); losses = getattr(t,"losses",0)
            final_rank = getattr(t,"final_standing", getattr(t,"playoff_pct",0))
            # Determine final standing: 1=champion, 2=runner-up, etc.
            # ESPN stores this in final_standing for completed seasons
            try: final_rank = int(final_rank) if final_rank else 0
            except: final_rank = 0

            season_teams.append({
                "name":t.team_name,"abbrev":t.team_abbrev,
                "wins":wins,"losses":losses,"ties":getattr(t,"ties",0),
                "pointsFor":round(getattr(t,"points_for",0),1),
                "pointsAgainst":round(getattr(t,"points_against",0),1),
                "finalRank":final_rank,
                "madePlayoffs": final_rank > 0 and final_rank <= PLAYOFF_SPOTS,
                "champion": final_rank == 1,
                "runnerUp": final_rank == 2,
            })

            # Update franchise data
            ok = owner_key(t)
            fd = franchise_data[ok]
            fd["names"].add(t.team_name)
            total = wins + losses
            win_pct = wins/total if total else 0
            fd["seasons"].append({
                "season":season,"name":t.team_name,
                "wins":wins,"losses":losses,
                "winPct":round(win_pct,3),"finalRank":final_rank,
                "madePlayoffs":final_rank>0 and final_rank<=PLAYOFF_SPOTS,
                "champion":final_rank==1,"runnerUp":final_rank==2,
            })
            fd["allW"] += wins; fd["allL"] += losses
            if final_rank == 1:         fd["championships"] += 1
            if final_rank == 2:         fd["runnerUps"] += 1
            if 0 < final_rank <= PLAYOFF_SPOTS: fd["playoffApps"] += 1
            if win_pct > 0.5:           fd["winningSeasons"] += 1
            fd["finishes"].append(final_rank if final_rank else TOTAL_TEAMS)

        season_teams.sort(key=lambda x:(-x["wins"],x["losses"]))
        for i,s in enumerate(season_teams): s["rank"] = i+1
        all_standings.append({"season":season,"teams":season_teams})

        # Matchups — use league.schedule (single fetch, much faster)
        cat_team_rec = defaultdict(lambda: defaultdict(lambda:{"W":0,"L":0,"T":0}))
        # team_name → {cat → {W,L,T}}

        name_to_ok = {t.team_name: owner_key(t) for t in teams}

        cur_streak = {t.team_name:{"W":0,"L":0} for t in teams}

        for m in league.schedule:
            ht = m.home_team; at = m.away_team
            if ht is None or at is None: continue
            hname = ht.team_name; aname = at.team_name
            period = getattr(m,"matchup_period",getattr(m,"matchupPeriodId",0))

            # Score — espn-api may give points or cat wins depending on league type
            hs = getattr(m,"home_score",0) or 0
            as_ = getattr(m,"away_score",0) or 0

            winner_raw = getattr(m,"winner","")
            if winner_raw == "home":   wname=hname
            elif winner_raw == "away": wname=aname
            else:                      wname="UNDECIDED"

            all_matchups.append({
                "season":season,"week":period,
                "home":hname,"away":aname,
                "homeCatW":hs,"awayCatW":as_,"winner":wname,
            })

            if wname not in ("UNDECIDED",""):
                h2h_wins[hname][aname]   += (1 if wname==hname else 0)
                h2h_wins[aname][hname]   += (1 if wname==aname else 0)
                h2h_losses[hname][aname] += (1 if wname==aname else 0)
                h2h_losses[aname][hname] += (1 if wname==hname else 0)

                # Update owner franchise streaks
                for tname, won in [(hname, wname==hname),(aname, wname==aname)]:
                    ok = name_to_ok.get(tname,"")
                    if not ok: continue
                    fd = franchise_data[ok]
                    if won:
                        cur_streak[tname]["W"] += 1; cur_streak[tname]["L"] = 0
                    else:
                        cur_streak[tname]["L"] += 1; cur_streak[tname]["W"] = 0
                    fd["streaks"]["maxW"] = max(fd["streaks"]["maxW"], cur_streak[tname]["W"])
                    fd["streaks"]["maxL"] = max(fd["streaks"]["maxL"], cur_streak[tname]["L"])

        # Cat by season (simplified — league.schedule doesn't always have per-cat detail)
        season_cat_teams = []
        for t in teams:
            season_cat_teams.append({
                "name":t.team_name,
                "catRecord":{cat:{"W":0,"L":0,"T":0} for cat in ALL_CATS}
            })
        cat_by_season.append({"season":season,"teams":season_cat_teams})

    # ── H2H formatted ─────────────────────────────────────────────────────────
    all_names = set()
    for n in h2h_wins: all_names.add(n)
    for n in h2h_losses: all_names.add(n)
    seen = set(); h2h_out = []
    for ta in sorted(all_names):
        for tb in sorted(all_names):
            if ta==tb: continue
            pair = tuple(sorted([ta,tb]))
            if pair in seen: continue
            seen.add(pair)
            h2h_out.append({
                "teamA":ta,"teamB":tb,
                "aWins":h2h_wins[ta][tb],"aLosses":h2h_losses[ta][tb],
            })

    save("history_standings.json",{"seasons":all_standings,"updated":updated})
    save("history_matchups.json", {"matchups":all_matchups,"updated":updated})
    save("history_h2h.json",      {"records":h2h_out,"teams":sorted(all_names),"updated":updated})
    save("history_cats.json",     {"seasons":cat_by_season,"cats":ALL_CATS,"updated":updated})

    return franchise_data, all_standings, h2h_out


# ── LEGACY SCORES + TEAM RESUMES ─────────────────────────────────────────────
def compute_legacy(franchise_data, all_standings, h2h_out):
    print("\n━━ Legacy Scores & Resumes ━━")
    updated = now_utc()

    # Compute legacy score per franchise
    legacy_teams = []
    for ok, fd in franchise_data.items():
        if not fd["seasons"]: continue

        n_seasons  = len(fd["seasons"])
        total_w    = fd["allW"]; total_l = fd["allL"]
        total_g    = total_w + total_l
        win_pct    = round(total_w/total_g,3) if total_g else 0

        # Consistency: lower std dev of finish = higher bonus
        finishes = [f for f in fd["finishes"] if f > 0]
        if len(finishes) > 1:
            mean_f = sum(finishes)/len(finishes)
            std_f  = math.sqrt(sum((f-mean_f)**2 for f in finishes)/len(finishes))
            consistency_bonus = round(max(0, LEGACY_CONSISTENCY * (1 - std_f / TOTAL_TEAMS)), 1)
        else:
            consistency_bonus = 0.0

        # Category balance: proxy via win% across the season histories
        # (real per-cat data sparse from API, use win rate consistency as proxy)
        # Teams that consistently win (balanced roster construction) get higher scores
        season_win_pcts = [s["winPct"] for s in fd["seasons"] if s["wins"]+s["losses"]>0]
        if season_win_pcts:
            mean_wp = sum(season_win_pcts)/len(season_win_pcts)
            std_wp  = math.sqrt(sum((w-mean_wp)**2 for w in season_win_pcts)/len(season_win_pcts)) if len(season_win_pcts)>1 else 0
            cat_balance_bonus = round(min(LEGACY_CAT_BALANCE, mean_wp*LEGACY_CAT_BALANCE*(1+0.3*(1-std_wp*4))), 1)
        else:
            cat_balance_bonus = 0.0

        legacy_score = (
            fd["championships"]  * LEGACY_CHAMP +
            fd["runnerUps"]      * LEGACY_RUNNER_UP +
            fd["playoffApps"]    * LEGACY_PLAYOFF +
            fd["winningSeasons"] * LEGACY_WIN_SEASON +
            consistency_bonus +
            cat_balance_bonus
        )

        # Trophy case
        trophies = []
        for _ in range(fd["championships"]):
            yr = next((s["season"] for s in fd["seasons"] if s.get("champion")), "?")
            trophies.append({"icon":"🏆","label":"Champion","detail":str(yr)})
        for _ in range(fd["runnerUps"]):
            yr = next((s["season"] for s in fd["seasons"] if s.get("runnerUp")), "?")
            trophies.append({"icon":"🥈","label":"Runner-Up","detail":str(yr)})
        if fd["playoffApps"] >= 4:
            trophies.append({"icon":"🏟️","label":"Playoff Stalwart","detail":f"{fd['playoffApps']}x appearances"})
        if consistency_bonus >= 35:
            trophies.append({"icon":"🎯","label":"Most Consistent","detail":f"Avg finish #{round(sum(finishes)/len(finishes),1) if finishes else '?'}"})
        if fd["streaks"]["maxW"] >= 6:
            trophies.append({"icon":"🔥","label":f"{fd['streaks']['maxW']}-Game Win Streak","detail":"All-time best"})
        if fd["winningSeasons"] == n_seasons and n_seasons >= 3:
            trophies.append({"icon":"📈","label":"Always Above .500","detail":f"All {n_seasons} seasons"})
        # Best single season
        best = max(fd["seasons"],key=lambda s:s["winPct"],default=None)
        if best and best["winPct"] >= 0.65:
            trophies.append({"icon":"⚡","label":"Elite Season","detail":f"{best['season']}: {best['wins']}-{best['losses']}"})

        # Worst/best season
        sorted_by_rank = sorted([s for s in fd["seasons"] if s["finalRank"]>0], key=lambda s:s["finalRank"])
        best_season  = sorted_by_rank[0]  if sorted_by_rank else None
        worst_season = sorted_by_rank[-1] if sorted_by_rank else None

        # H2H vs all opponents (by team name, not owner — for the history tab)
        team_names_hist = list(fd["names"])
        my_h2h = []
        for rec in h2h_out:
            if rec["teamA"] in team_names_hist or rec["teamB"] in team_names_hist:
                if rec["teamA"] in team_names_hist:
                    opp=rec["teamB"]; w=rec["aWins"]; l=rec["aLosses"]
                else:
                    opp=rec["teamA"]; w=rec["aLosses"]; l=rec["aWins"]
                my_h2h.append({"opponent":opp,"wins":w,"losses":l})
        my_h2h.sort(key=lambda x:-x["wins"])

        primary_name = max(fd["names"], key=lambda n: sum(1 for s in fd["seasons"] if s["name"]==n), default=ok)

        legacy_teams.append({
            "ownerKey":      ok,
            "name":          primary_name,
            "allNames":      list(fd["names"]),
            "legacyScore":   round(legacy_score, 1),
            "components": {
                "championships":    fd["championships"],
                "runnerUps":        fd["runnerUps"],
                "playoffApps":      fd["playoffApps"],
                "winningSeasons":   fd["winningSeasons"],
                "consistencyBonus": consistency_bonus,
                "catBalanceBonus":  cat_balance_bonus,
            },
            "allTimeW":    total_w,
            "allTimeL":    total_l,
            "winPct":      win_pct,
            "seasonsTracked": n_seasons,
            "seasonHistory":  sorted(fd["seasons"],key=lambda s:s["season"]),
            "trophyCase":     trophies,
            "bestSeason":     best_season,
            "worstSeason":    worst_season,
            "longestWinStreak":  fd["streaks"]["maxW"],
            "longestLossStreak": fd["streaks"]["maxL"],
            "h2hRecords":    my_h2h,
        })

    legacy_teams.sort(key=lambda x:-x["legacyScore"])
    for i,t in enumerate(legacy_teams):
        t["legacyRank"] = i+1

    save("legacy_scores.json",  {"teams":legacy_teams,"updated":updated})
    save("team_resumes.json",   {"teams":legacy_teams,"updated":updated})
    print(f"  ✅  Legacy scores for {len(legacy_teams)} franchises")
    return legacy_teams


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"🔄  ESPN Fetcher | League {LEAGUE_ID} | Season {CURRENT_SEASON}")
    fetch_current_season()
    franchise_data, all_standings, h2h_out = fetch_history()
    compute_legacy(franchise_data, all_standings, h2h_out)
    print("\n✅  All done.")
