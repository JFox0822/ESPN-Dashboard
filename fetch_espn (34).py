#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Data Fetcher (espn-api version)
"""

import json
import os
import sys
from datetime import datetime, timezone

LEAGUE_ID = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
ESPN_S2   = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")
SEASON    = 2026

# ESPN stat ID → label
STAT_MAP = {
    "20": "R", "21": "RBI", "5": "HR", "23": "SB", "27": "Kbat",
    "2": "AVG", "17": "OPS", "34": "IP", "41": "H", "48": "K",
    "63": "QS", "47": "ERA", "53": "WHIP", "57": "SV", "83": "HLD",
}

# Categories scored in this league (hitting / pitching)
HIT_CATS = ["R", "RBI", "HR", "SB", "Kbat", "AVG", "OPS"]
PIT_CATS = ["IP", "H", "K", "QS", "ERA", "WHIP", "SV", "HLD"]
ALL_CATS = HIT_CATS + PIT_CATS
LOWER_BETTER = {"ERA", "WHIP", "H", "Kbat"}   # lower = better for these

# Maps ESPN team name keywords → RecordBook short name (owner surname)
# Used to link live ESPN data to historical RB data
ESPN_TO_RB = {
    "ryder":     "Boyce",
    "manatee":   "TC",
    "sea":       "Leo",
    "mohawk":    "Tim",
    "degenerate":"Greene",
    "pickle":    "Schoon",
    "bomber":    "Sponny",
    "coqui":     "Jacob",
    "tugger":    "Slater",
    "veloci":    "Eriole",
    "general":   "Mion",
    "bison":     "Bert",
}

def espn_to_rb(team_name):
    """Map an ESPN team name to its RecordBook short name."""
    lower = (team_name or "").lower()
    for keyword, rb in ESPN_TO_RB.items():
        if keyword in lower:
            return rb
    return None

SLOT_MAP = {
    0:"C",1:"1B",2:"2B",3:"3B",4:"SS",5:"OF",6:"2B/SS",7:"1B/3B",
    8:"LF",9:"CF",10:"RF",11:"DH",12:"UTIL",13:"SP",14:"RP",15:"P",
    16:"BE",17:"IL",18:"IL10",19:"IL60",20:"NA",21:"BE",22:"IL",
}

POS_MAP = {
    1:"C",2:"1B",3:"2B",4:"3B",5:"SS",6:"OF",
    7:"2B/SS",8:"1B/3B",9:"P",10:"SP",11:"RP",12:"DH",
}

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

def fmt_val(lbl, v):
    """Format a stat value for display."""
    if v is None:
        return "—"
    if lbl in {"AVG", "OPS"}:
        return f"{float(v):.3f}"
    if lbl in {"ERA", "WHIP"}:
        return f"{float(v):.2f}"
    if lbl == "IP":
        return f"{float(v):.1f}"
    return str(int(round(float(v)))) if isinstance(v, float) and v == int(v) else str(round(float(v), 1))

def sum_lineup_stats(lineup):
    """
    Sum stats across all players in a lineup (non-bench only).
    lineup is a list of BoxPlayer objects from espn-api.
    Each BoxPlayer has .stats dict (stat_id_str -> value) and .slot_position.
    """
    totals = {}
    bench_slots = {"BE", "BN", "IL", "IL10", "IL60", "NA"}

    # Weighted stats need special handling (AVG, OPS, ERA, WHIP)
    ab_total = 0       # for AVG
    hits_total = 0
    ops_num = 0
    ip_total = 0       # for ERA, WHIP
    er_total = 0
    walks_hits_total = 0

    for player in lineup:
        slot = getattr(player, 'slot_position', '') or ''
        if slot in bench_slots:
            continue

        pstats = getattr(player, 'stats', {}) or {}
        # stats is keyed by scoring_period -> {stat_id: value} or directly {stat_id: value}
        # In espn-api baseball, it's usually player.stats = {period: {statId: val}}
        # Get current period stats
        raw = {}
        if isinstance(pstats, dict):
            # Try to find current period - could be nested or flat
            for key, val in pstats.items():
                if isinstance(val, dict):
                    raw.update(val)
                else:
                    raw[key] = val

        for stat_id_raw, val in raw.items():
            lbl = STAT_MAP.get(str(stat_id_raw))
            if lbl is None or val is None:
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue

            if lbl not in totals:
                totals[lbl] = 0.0
            totals[lbl] += v

    return totals

def get_team_cats(box_side_team, box_obj, is_home):
    """
    Try multiple methods to get team category totals for a matchup side.
    Returns dict: {cat_label: float_value}
    """
    result = {}

    # Method 1: Sum from lineup players
    lineup_attr = 'home_lineup' if is_home else 'away_lineup'
    lineup = getattr(box_obj, lineup_attr, []) or []
    if lineup:
        result = sum_lineup_stats(lineup)
        if result:
            print(f"    Method 1 (lineup sum): got {len(result)} cats")
            return result

    # Method 2: Direct stats on the team object (period stats)
    if box_side_team:
        for attr in ['stats', 'season_stats', 'currentPeriodStats', 'valuesByStat']:
            raw = getattr(box_side_team, attr, None)
            if raw and isinstance(raw, dict):
                for k, v in raw.items():
                    lbl = STAT_MAP.get(str(k))
                    if lbl and v is not None:
                        try:
                            result[lbl] = float(v)
                        except (TypeError, ValueError):
                            pass
                if result:
                    print(f"    Method 2 ({attr}): got {len(result)} cats")
                    return result

    # Method 3: raw _data on box score if available
    try:
        raw_data = getattr(box_obj, '_data', {}) or {}
        side_key = 'home' if is_home else 'away'
        side_data = raw_data.get(side_key, {})
        cat_data = side_data.get('cumulativeScore', {}).get('scoreByStat', {})
        for stat_id, stat_info in cat_data.items():
            lbl = STAT_MAP.get(str(stat_id))
            if lbl:
                val = stat_info.get('score', stat_info.get('value'))
                if val is not None:
                    try:
                        result[lbl] = float(val)
                    except (TypeError, ValueError):
                        pass
        if result:
            print(f"    Method 3 (_data): got {len(result)} cats")
            return result
    except Exception:
        pass

    return result

def build_side(team, cat_wins, cat_losses, cat_ties, cats_raw, team_map):
    """Build a matchup side dict with formatted category data."""
    if team is None:
        return {"teamId": None, "team": "BYE", "abbrev": "BYE",
                "catWins": 0, "catLoss": 0, "catTies": 0, "categories": {}}
    tm = team_map.get(team.team_id, {})
    return {
        "teamId":     team.team_id,
        "team":       team.team_name,
        "abbrev":     tm.get("abbrev", ""),
        "catWins":    cat_wins,
        "catLoss":    cat_losses,
        "catTies":    cat_ties,
        "categories": cats_raw,  # {lbl: {value, result}}
    }

def resolve_cat_results(home_raw, away_raw):
    """
    Given raw {lbl: float} for home and away,
    return {lbl: {value: str, result: WIN/LOSS/TIE}} for each side.
    """
    home_cats, away_cats = {}, {}
    all_lbls = set(list(home_raw.keys()) + list(away_raw.keys()))
    for lbl in all_lbls:
        hv = home_raw.get(lbl)
        av = away_raw.get(lbl)
        hfmt = fmt_val(lbl, hv)
        afmt = fmt_val(lbl, av)
        if hv is None or av is None:
            h_res = a_res = "TIE"
        else:
            lower = lbl in LOWER_BETTER
            if abs(float(hv) - float(av)) < 0.0001:
                h_res = a_res = "TIE"
            elif (float(hv) < float(av)) == lower:
                h_res, a_res = "WIN", "LOSS"
            else:
                h_res, a_res = "LOSS", "WIN"
        home_cats[lbl] = {"value": hfmt, "result": h_res}
        away_cats[lbl] = {"value": afmt, "result": a_res}
    return home_cats, away_cats

def main():
    try:
        from espn_api.baseball import League
    except ImportError:
        print("❌  espn-api not installed.", file=sys.stderr)
        sys.exit(1)

    print(f"🔄  Connecting to ESPN league {LEAGUE_ID}, season {SEASON}…")
    kwargs = {"league_id": LEAGUE_ID, "year": SEASON}
    if ESPN_S2 and ESPN_SWID:
        kwargs["espn_s2"] = ESPN_S2
        kwargs["swid"]    = ESPN_SWID
        print("  🔐  Using ESPN_S2 + SWID cookies")

    try:
        league = League(**kwargs)
    except Exception as e:
        print(f"❌  Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    updated = now_utc()
    current_week = league.current_week
    print(f"  📅  ESPN current_week: {current_week}")

    # Use currentMatchupPeriod (actual matchup week) rather than current_week
    # which can tick forward before scoring ends.
    matchup_period = getattr(league, 'currentMatchupPeriod',
                     getattr(league, 'current_matchup_period', current_week))
    print(f"  📅  currentMatchupPeriod: {matchup_period}")

    # Validate by checking if box scores have actual activity (catWins > 0 somewhere)
    def has_scores(boxes):
        return any(
            (getattr(b, 'home_wins', 0) or 0) + (getattr(b, 'away_wins', 0) or 0) > 0
            for b in (boxes or [])
        )

    def get_active_week(league, week):
        """Try week, then week-1, pick whichever has actual scores."""
        boxes_w = []
        try:
            boxes_w = league.box_scores(week)
        except Exception as e:
            print(f"  ⚠️  box_scores({week}) failed: {e}")

        if has_scores(boxes_w):
            return week, boxes_w

        if week > 1:
            boxes_prev = []
            try:
                boxes_prev = league.box_scores(week - 1)
            except Exception as e:
                print(f"  ⚠️  box_scores({week-1}) failed: {e}")
            if has_scores(boxes_prev):
                print(f"  ⚠️  Week {week} has no scores — using week {week-1}")
                return week - 1, boxes_prev

            # Neither has scores yet (very start of season) — use the lower week
            if boxes_prev and any(getattr(b, 'home_team', None) for b in boxes_prev):
                print(f"  ⚠️  No scores yet — using week {week-1} (pre-scoring)")
                return week - 1, boxes_prev

        print(f"  ℹ️   Using week {week} (no prior week or no data)")
        return week, boxes_w

    scoring_week, prefetched_boxes = get_active_week(league, matchup_period)
    print(f"  📅  Scoring week resolved to: {scoring_week}")

    # ── Team map ────────────────────────────────────────────────────────────────
    team_map = {}
    for t in league.teams:
        raw_owners = getattr(t, "owners", []) or []
        owners = [o.get("displayName", o.get("firstName","?")) if isinstance(o, dict) else str(o) for o in raw_owners]
        team_map[t.team_id] = {
            "id":     t.team_id,
            "name":   t.team_name,
            "abbrev": getattr(t, "team_abbrev", t.team_name[:3].upper()),
            "owners": owners,
            "logo":   getattr(t, "logo_url", getattr(t, "logo", "")),
            "rbName": espn_to_rb(t.team_name) or "",  # RecordBook short name
        }

    # ── Standings ───────────────────────────────────────────────────────────────
    standings = []
    for t in league.teams:
        tm = team_map[t.team_id]
        standings.append({
            **tm,
            "wins":          t.wins,
            "losses":        t.losses,
            "ties":          t.ties,
            "pointsFor":     round(getattr(t, "points_for", 0) or 0, 1),
            "pointsAgainst": round(getattr(t, "points_against", 0) or 0, 1),
            "streak":        getattr(t, "streak_length", 0),
            "streakType":    getattr(t, "streak_type", ""),
        })
    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1
    save("standings.json", {"week": scoring_week, "standings": standings, "updated": updated})

    # ── Matchups: use espn-api's internal request mechanism ──────────────────
    # box_scores() fails due to abstract class bug in espn-api baseball.
    # Instead we replicate the exact API call espn-api makes internally,
    # using league.espn_request which already has auth headers/cookies set.
    matchups_out = []
    try:
        req = league.espn_request  # EspnFantasyRequests object

        STAT_KEYS = {
            "20":"R","21":"RBI","5":"HR","23":"SB","27":"Kbat",
            "2":"AVG","17":"OPS","34":"IP","41":"H","48":"K",
            "63":"QS","47":"ERA","53":"WHIP","57":"SV","83":"HLD",
        }
        LOWER_CATS = {"ERA","WHIP","H","Kbat"}

        def fmt_v(v, lbl):
            if v is None: return "—"
            try:
                f = float(v)
                if lbl in {"AVG","OPS"}: return f"{f:.3f}"
                if lbl in {"ERA","WHIP"}: return f"{f:.2f}"
                if lbl == "IP": return f"{f:.1f}"
                return str(int(f)) if f == int(f) else str(round(f,1))
            except: return str(v)

        id_to_name = {t.team_id: t.team_name for t in league.teams}

        def parse_and_load(sp):
            """Fetch schedule data for scoring period sp, return parsed matchups."""
            # espn-api's get() takes extend + keyword params
            # It builds: BASE_URL + extend + ?param=val&...
            try:
                data = req.get('', view=['mMatchup', 'mMatchupScore'],
                               scoringPeriodId=sp)
            except TypeError:
                # Some versions use positional: get(extend, params_dict)
                try:
                    data = req.get('', {'view': ['mMatchup', 'mMatchupScore'],
                                        'scoringPeriodId': sp})
                except Exception:
                    # Last resort: get the underlying session and call it directly
                    s = getattr(req, 'session', getattr(req, '_session', None))
                    base = getattr(req, 'FANTASY_BASE_ENDPOINT',
                           getattr(req, 'base_url',
                           f'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}'))
                    if s:
                        r = s.get(base, params={'view': ['mMatchup','mMatchupScore'],
                                                'scoringPeriodId': sp})
                        data = r.json() if hasattr(r,'json') else r
                    else:
                        raise RuntimeError("No usable session found")

            print(f"  API sp={sp}: got type={type(data).__name__}, "
                  f"keys={list(data.keys())[:5] if isinstance(data,dict) else '?'}")

            schedule = data.get('schedule', []) if isinstance(data, dict) else []
            print(f"  Schedule entries: {len(schedule)}, "
                  f"matchupPeriodIds: {sorted(set(m.get('matchupPeriodId',0) for m in schedule[:20]))}")

            results = []
            for m in schedule:
                if m.get('matchupPeriodId') != sp:
                    continue
                home_d = m.get('home', {})
                away_d = m.get('away', {})
                if not home_d.get('teamId') or not away_d.get('teamId'):
                    continue

                def parse_side(side):
                    tid = side.get('teamId')
                    tname = id_to_name.get(tid, f'Team {tid}')
                    tm = team_map.get(tid, {})
                    stats = {}
                    cum = side.get('cumulativeScore', {})
                    for stat_id, info in cum.get('scoreByStat', {}).items():
                        lbl = STAT_KEYS.get(str(stat_id))
                        if lbl:
                            v = info.get('score', info.get('value'))
                            if v is not None:
                                try: stats[lbl] = float(v)
                                except: pass
                    cat_wins   = int(cum.get('wins',   0) or 0)
                    cat_losses = int(cum.get('losses', 0) or 0)
                    return {'teamId': tid, 'team': tname,
                            'abbrev': tm.get('abbrev',''), 'rbName': tm.get('rbName',''),
                            'catWins': cat_wins, 'catLoss': cat_losses, 'catTies': 0,
                            'stats': stats}

                hs = parse_side(home_d)
                as_ = parse_side(away_d)

                # Per-category win/loss
                hc, ac = {}, {}
                for lbl in set(list(hs['stats']) + list(as_['stats'])):
                    hv = hs['stats'].get(lbl)
                    av = as_['stats'].get(lbl)
                    if hv is None or av is None: continue
                    lower = lbl in LOWER_CATS
                    if abs(hv-av) < 0.0001: hr = ar = 'TIE'
                    elif (hv < av) == lower: hr, ar = 'WIN', 'LOSS'
                    else:                     hr, ar = 'LOSS', 'WIN'
                    hc[lbl] = {'value': fmt_v(hv,lbl), 'result': hr}
                    ac[lbl] = {'value': fmt_v(av,lbl), 'result': ar}

                hs['categories'] = hc; del hs['stats']
                as_['categories'] = ac; del as_['stats']
                hw = hs['catWins'] > as_['catWins']
                aw = as_['catWins'] > hs['catWins']
                results.append({
                    'home': hs, 'away': as_,
                    'leader': hs['team'] if hw else (as_['team'] if aw else 'Tied'),
                    'winner': 'UNDECIDED',
                })
                print(f"    ✓ {hs['team']} ({hs['catWins']}) vs {as_['team']} ({as_['catWins']})")
            return results

        # Try scoring period 1 first, then 2
        for sp in [1, 2]:
            try:
                results = parse_and_load(sp)
                if results:
                    matchups_out = results
                    scoring_week = sp
                    print(f"  ✅  {len(matchups_out)} matchups from sp={sp}")
                    break
                else:
                    print(f"  ⚠️  sp={sp}: 0 matchups found")
            except Exception as e:
                import traceback
                print(f"  ⚠️  sp={sp} failed: {e}")
                traceback.print_exc()

    except Exception as e:
        import traceback
        print(f"  ⚠️  Matchup block failed: {e}")
        traceback.print_exc()
    save("matchups.json", {
        "week":     scoring_week,
        "matchups": matchups_out,
        "updated":  updated,
    })

    # ── Team stats (season totals) ──────────────────────────────────────────────
    team_stats = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        stats = {}
        raw_stats = getattr(t, "stats", {}) or getattr(t, "valuesByStat", {}) or {}
        for k, v in raw_stats.items():
            lbl = STAT_MAP.get(str(k))
            if lbl and v is not None:
                try:
                    stats[lbl] = round(float(v), 3)
                except (TypeError, ValueError):
                    pass
        team_stats.append({**tm, "wins": s_rec.get("wins",0),
                           "losses": s_rec.get("losses",0), "stats": stats})
    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── Rosters ─────────────────────────────────────────────────────────────────
    rosters_out = []
    for t in league.teams:
        tm = team_map[t.team_id]
        players = []
        for p in (t.roster or []):
            slot_id     = getattr(p, "lineupSlot", getattr(p, "slot_id", 16))
            slot_label  = SLOT_MAP.get(slot_id, "BE") if isinstance(slot_id, int) else str(slot_id)
            pos_id      = getattr(p, "defaultPositionId", getattr(p, "position_id", 0)) or 0
            primary_pos = POS_MAP.get(pos_id, "?")
            eligible_ids = getattr(p, "eligibleSlots", []) or []
            eligible_str = "/".join(list(dict.fromkeys(
                SLOT_MAP.get(s,"") for s in eligible_ids
                if SLOT_MAP.get(s,"") not in ("BE","IL","IL10","IL60","NA","")
            )))[:20]
            is_pitcher  = pos_id in (9, 10, 11, 13)
            inj_status  = getattr(p, "injuryStatus", "ACTIVE") or "ACTIVE"
            acq_type    = (getattr(p, "acquisitionType", "") or "").upper()
            draft_round = getattr(p, "draftRound", 0) or 0
            is_keeper   = (draft_round >= 13) or (draft_round == 0 and
                           any(x in acq_type for x in ["FREE","WAIVER","FA"]))
            players.append({
                "name":            getattr(p, "name", "Unknown"),
                "slot":            slot_label,
                "position":        primary_pos,
                "eligible":        eligible_str,
                "isPitcher":       is_pitcher,
                "injStatus":       inj_status,
                "stats":           {},
                "tier":            "",
                "acquisitionType": acq_type,
                "draftRound":      draft_round,
                "keeperEligible":  is_keeper,
            })
        rosters_out.append({**tm, "teamId": t.team_id, "players": players})
    save("rosters.json", {"season": SEASON, "week": scoring_week,
                          "teams": rosters_out, "updated": updated})

    # ── Power Rankings ──────────────────────────────────────────────────────────
    ts_map = {t["id"]: t.get("stats",{}) for t in team_stats}
    power  = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        power.append({"id": t.team_id, "name": t.team_name,
                      "abbrev": tm.get("abbrev",""),
                      "overallW": s_rec.get("wins",0), "overallL": s_rec.get("losses",0),
                      "overallRank": s_rec.get("rank",0), "catRanks": {}})
    for cat in ALL_CATS:
        vals = [(p["id"], ts_map.get(p["id"],{}).get(cat)) for p in power]
        vals = [(pid,v) for pid,v in vals if v is not None]
        if len(vals) < 2: continue
        vals.sort(key=lambda x: x[1], reverse=(cat not in LOWER_BETTER))
        for rank,(pid,_) in enumerate(vals, 1):
            for p in power:
                if p["id"] == pid: p["catRanks"][cat] = rank
    power.sort(key=lambda x: (-x["overallW"], x["overallL"]))
    for i,p in enumerate(power): p["pwRank"] = i+1; p["rankDelta"] = 0
    save("power_rankings.json", {"week": scoring_week, "rankings": power, "updated": updated})

    # ── Meta ────────────────────────────────────────────────────────────────────
    save("meta.json", {
        "leagueName":  getattr(league.settings, "name", "The League"),
        "season":      SEASON,
        "currentWeek": scoring_week,
        "teamCount":   len(league.teams),
        "teams":       [{"id":t["id"],"name":t["name"],"abbrev":t["abbrev"]} for t in standings],
        "updated":     updated,
    })

    # ── KOH ─────────────────────────────────────────────────────────────────────
    save("koh.json", update_koh(league, current_week, team_map, updated))

    # ── Draft + Keepers ──────────────────────────────────────────────────────────
    draft_picks, keeper_by_name = fetch_draft_and_keepers(league, team_map)
    save("draft.json", {"season": SEASON, "picks": draft_picks,
                        "keeperEligible": keeper_by_name, "updated": updated})

    print(f"\n🏆  Done! Week {current_week}.")


def update_koh(league, current_week, team_map, updated):
    koh_path  = "data/koh.json"
    all_names = {tid: tm["name"] for tid, tm in team_map.items()}
    state = None
    if os.path.exists(koh_path):
        with open(koh_path) as f:
            state = json.load(f)
        if state.get("season") != SEASON:
            state = None
    if state is None:
        state = {"season": SEASON, "active": list(team_map.keys()),
                 "eliminated": [], "champions": [], "currentRound": 1,
                 "processedWeeks": [], "history": [], "teamNames": all_names}

    for week in range(1, current_week):
        if week in state["processedWeeks"]:
            continue
        try:
            boxes = league.box_scores(week)
        except Exception as e:
            print(f"  ⚠️  KOH week {week}: {e}"); continue

        all_scores, losers = {}, []
        for b in boxes:
            ht, at = b.home_team, b.away_team
            if not ht or not at: continue
            hid, aid = ht.team_id, at.team_id
            hw = getattr(b,'home_wins',0) or 0
            aw = getattr(b,'away_wins',0) or 0
            all_scores[hid] = hw; all_scores[aid] = aw
            if hw < aw: losers.append({"teamId":hid,"team":all_names.get(hid,str(hid)),"catWins":hw})
            elif aw < hw: losers.append({"teamId":aid,"team":all_names.get(aid,str(aid)),"catWins":aw})

        active_set    = set(state["active"])
        active_losers = [l for l in losers if l["teamId"] in active_set]
        week_entry    = {"week":week,"round":state["currentRound"],
                         "allScores":{str(k):v for k,v in all_scores.items()},
                         "losers":[{"team":l["team"],"catWins":l["catWins"]} for l in losers],
                         "eliminated":[],"allActive":{all_names.get(t,str(t)):True for t in state["active"]}}

        if active_losers:
            min_cats = min(l["catWins"] for l in active_losers)
            for t in [l for l in active_losers if l["catWins"]==min_cats]:
                if t["teamId"] in state["active"]: state["active"].remove(t["teamId"])
                e = {"teamId":t["teamId"],"team":t["team"],"week":week,
                     "catWins":t["catWins"],"round":state["currentRound"]}
                state["eliminated"].append(e); week_entry["eliminated"].append(e)
            week_entry["allActive"] = {all_names.get(t,str(t)):t in state["active"] for t in team_map}
            if len(state["active"]) <= 1:
                if len(state["active"]) == 1:
                    wid = state["active"][0]
                    wname = all_names.get(wid,str(wid))
                    state["champions"].append({"teamId":wid,"team":wname,"week":week,"round":state["currentRound"]})
                    week_entry["champion"] = wname
                state["active"] = list(team_map.keys()); state["currentRound"] += 1

        state["processedWeeks"].append(week); state["history"].append(week_entry)
    state["updated"] = updated
    return state


def fetch_draft_and_keepers(league, team_map):
    draft_picks, keeper_eligible = [], {}
    try:
        for pick in (league.draft or []):
            try:
                tid       = pick.team.team_id if hasattr(pick,'team') and pick.team else None
                pname     = getattr(pick,'playerName',None) or getattr(pick,'player_name','Unknown')
                round_num = getattr(pick,'round_num',0) or 0
                round_pk  = getattr(pick,'round_pick',0) or 0
                tname     = team_map.get(tid,{}).get("name","?") if tid else "?"
                draft_picks.append({"round":round_num,"pick":round_pk,
                                    "overall":(round_num-1)*len(team_map)+round_pk,
                                    "teamId":tid,"team":tname,"player":pname,
                                    "keeperEligible":round_num>=13})
                if round_num >= 13 and tid:
                    keeper_eligible.setdefault(tid,[]).append({"name":pname,"source":f"Rd {round_num}"})
            except Exception as pe:
                print(f"  ⚠️  Draft pick: {pe}")
    except Exception as e:
        print(f"  ⚠️  Draft: {e}")

    try:
        for t in league.teams:
            for p in (t.roster or []):
                acq = (getattr(p,'acquisitionType','') or '').upper()
                if any(x in acq for x in ['FREE','WAIVER','FA']):
                    keeper_eligible.setdefault(t.team_id,[])
                    pname = getattr(p,'name','Unknown')
                    if not any(x['name']==pname for x in keeper_eligible[t.team_id]):
                        keeper_eligible[t.team_id].append({"name":pname,"source":"FA/Waiver"})
    except Exception as e:
        print(f"  ⚠️  FA data: {e}")

    return draft_picks, {team_map.get(tid,{}).get("name",f"Team {tid}"): pl
                         for tid, pl in keeper_eligible.items()}


if __name__ == "__main__":
    main()
