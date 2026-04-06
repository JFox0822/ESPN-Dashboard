#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Data Fetcher
Fixes: week detection, KOH (no box_scores), standings cat W/L/streak/allplay,
       history_matchups.json, cat_wl_2026.json
"""

import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

LEAGUE_ID = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
ESPN_S2   = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")
SEASON    = 2026

STAT_MAP = {
    "20": "R", "21": "RBI", "5": "HR", "23": "SB", "27": "Kbat",
    "2": "AVG", "18": "OPS", "34": "IP", "37": "H", "48": "K",
    "63": "QS", "47": "ERA", "41": "WHIP", "60": "SVHD",
}

HIT_CATS = ["R", "RBI", "HR", "SB", "Kbat", "AVG", "OPS"]
PIT_CATS = ["IP", "H", "K", "QS", "ERA", "WHIP", "SV", "HLD"]
ALL_CATS = HIT_CATS + PIT_CATS
LOWER_BETTER = {"ERA", "WHIP", "H", "Kbat"}

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

def extract_svhd(sbs_any, tname):
    """stat83 = authoritative SV+HLD. Fall back to max(60, 57)."""
    def get_score(sid):
        info = sbs_any.get(sid)
        if isinstance(info, dict):
            v = info.get('score', info.get('value'))
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return 0.0
    svhd83 = get_score('83')
    svhd60 = get_score('60')
    svhd57 = get_score('57')
    result = svhd83 if svhd83 > 0 else max(svhd60, svhd57)
    print(f"    SVHD: stat83={svhd83} stat60={svhd60} stat57={svhd57} → using {result} for {tname}")
    return result

# ── All-play simulation from allWeeks ─────────────────────────────────────────
def compute_allplay(all_weeks_out):
    ap = defaultdict(lambda: {'w': 0, 'l': 0, 't': 0})
    for wk_str, wk_list in all_weeks_out.items():
        if any(m.get('winner') == 'UNDECIDED' for m in wk_list):
            continue
        team_cats = []
        for m in wk_list:
            for side_key in ['home', 'away']:
                side = m[side_key]
                tid = side.get('teamId')
                if not tid:
                    continue
                cats = {}
                for cat, info in (side.get('categories') or {}).items():
                    v = info.get('value')
                    if v and v != '—':
                        try:
                            cats[cat] = float(v)
                        except (ValueError, TypeError):
                            pass
                team_cats.append({'teamId': tid, 'cats': cats})
        if not team_cats:
            continue
        all_cat_keys = set()
        for t in team_cats:
            all_cat_keys.update(t['cats'].keys())
        for i, ta in enumerate(team_cats):
            for j, tb in enumerate(team_cats):
                if i == j:
                    continue
                a_w = b_w = 0
                for cat in all_cat_keys:
                    av = ta['cats'].get(cat)
                    bv = tb['cats'].get(cat)
                    if av is None or bv is None:
                        continue
                    lower = cat in LOWER_BETTER
                    if abs(av - bv) < 0.0001:
                        pass
                    elif (av < bv) == lower:
                        a_w += 1
                    else:
                        b_w += 1
                tid = ta['teamId']
                if a_w > b_w:
                    ap[tid]['w'] += 1
                elif b_w > a_w:
                    ap[tid]['l'] += 1
                else:
                    ap[tid]['t'] += 1
    return ap

# ── Category W-L from allWeeks ────────────────────────────────────────────────
def compute_cat_wl(all_weeks_out):
    cat_wl = defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 't': 0}))
    for wk_str, wk_list in all_weeks_out.items():
        for m in wk_list:
            if m.get('winner') == 'UNDECIDED':
                continue
            for side_key in ['home', 'away']:
                side = m[side_key]
                name = side.get('rbName') or side.get('team', '')
                for cat, info in (side.get('categories') or {}).items():
                    result = info.get('result', '')
                    if result == 'WIN':
                        cat_wl[name][cat]['w'] += 1
                    elif result == 'LOSS':
                        cat_wl[name][cat]['l'] += 1
                    elif result == 'TIE':
                        cat_wl[name][cat]['t'] += 1
    return cat_wl

# ── Season stat totals from allWeeks ─────────────────────────────────────────
def compute_season_stats(all_weeks_out):
    """
    Compute per-team season stat totals from allWeeks category values.
    Counting stats summed; ERA/WHIP weighted by IP; AVG/OPS weekly average.
    Returns: {teamId: {cat: value}}
    """
    COUNTING = {'R', 'HR', 'RBI', 'SB', 'Kbat', 'H', 'K', 'QS', 'SVHD'}
    accum = defaultdict(lambda: defaultdict(lambda: {'sum': 0.0, 'ip_sum': 0.0, 'n': 0}))
    ip_thirds_total = defaultdict(int)

    for wk_str, wk_list in all_weeks_out.items():
        for m in wk_list:
            if m.get('winner') == 'UNDECIDED':
                continue
            for side_key in ['home', 'away']:
                side = m[side_key]
                tid = side.get('teamId')
                if not tid:
                    continue
                cats = side.get('categories') or {}

                # Parse IP as decimal innings for weighting ERA/WHIP
                ip_dec = 0.0
                if 'IP' in cats:
                    v_str = cats['IP'].get('value', '—')
                    if v_str and v_str != '—':
                        try:
                            ip_f = float(v_str)
                            inn = int(ip_f)
                            thirds = round((ip_f - inn) * 10)  # 0.1→1, 0.2→2
                            ip_dec = inn + thirds / 3.0
                            ip_thirds_total[tid] += inn * 3 + thirds
                        except: pass

                for cat, info in cats.items():
                    v_str = info.get('value', '—')
                    if not v_str or v_str == '—':
                        continue
                    try:
                        v = float(v_str)
                    except (ValueError, TypeError):
                        continue

                    a = accum[tid][cat]
                    if cat in COUNTING:
                        a['sum'] += v
                        a['n'] = 1  # flag as counting (not averaged)
                    elif cat == 'IP':
                        pass  # tracked via ip_thirds_total
                    elif cat in ('ERA', 'WHIP') and ip_dec > 0:
                        a['sum'] += v * ip_dec   # weighted by innings
                        a['ip_sum'] += ip_dec
                        a['n'] += 1
                    elif cat in ('AVG', 'OPS'):
                        a['sum'] += v
                        a['n'] += 1

    result = {}
    for tid in set(list(accum.keys()) + list(ip_thirds_total.keys())):
        stats = {}
        thirds = ip_thirds_total.get(tid, 0)
        if thirds > 0:
            stats['IP'] = float(f"{thirds // 3}.{thirds % 3}")

        for cat, a in accum.get(tid, {}).items():
            if cat == 'IP':
                continue
            if cat in COUNTING and a['n']:
                stats[cat] = int(round(a['sum']))
            elif cat in ('ERA', 'WHIP') and a['ip_sum'] > 0:
                stats[cat] = round(a['sum'] / a['ip_sum'], 2)
            elif cat in ('AVG', 'OPS') and a['n'] > 0:
                decimals = 3
                stats[cat] = round(a['sum'] / a['n'], decimals)

        result[tid] = stats

    return result

# ── KOH from schedule (no box_scores needed) ──────────────────────────────────
def update_koh_from_schedule(all_weeks_out, team_map, all_id_to_name, updated):
    koh_path = "data/koh.json"
    all_names = {tid: tm["name"] for tid, tm in team_map.items()}
    state = None
    if os.path.exists(koh_path):
        with open(koh_path) as f:
            state = json.load(f)
        if state.get("season") != SEASON:
            state = None
    if state is None:
        state = {
            "season": SEASON,
            "active": list(team_map.keys()),
            "eliminated": [], "champions": [],
            "currentRound": 1, "processedWeeks": [],
            "history": [], "teamNames": all_names,
        }

    sorted_weeks = sorted([int(w) for w in all_weeks_out.keys()])
    for week in sorted_weeks:
        if week in state["processedWeeks"]:
            continue
        wk_matchups = all_weeks_out.get(str(week), [])
        if not wk_matchups or any(m.get('winner') == 'UNDECIDED' for m in wk_matchups):
            continue

        all_scores, losers = {}, []
        for m in wk_matchups:
            home = m.get('home', {})
            away = m.get('away', {})
            hid = home.get('teamId')
            aid = away.get('teamId')
            if not hid or not aid:
                continue
            hw = home.get('catWins', 0)
            aw = away.get('catWins', 0)
            all_scores[hid] = hw
            all_scores[aid] = aw
            if hw < aw:
                losers.append({"teamId": hid, "team": all_names.get(hid, str(hid)), "catWins": hw})
            elif aw < hw:
                losers.append({"teamId": aid, "team": all_names.get(aid, str(aid)), "catWins": aw})

        active_set = set(state["active"])
        active_losers = [l for l in losers if l["teamId"] in active_set]
        week_entry = {
            "week": week, "round": state["currentRound"],
            "allScores": {str(k): v for k, v in all_scores.items()},
            "losers": [{"team": l["team"], "catWins": l["catWins"]} for l in losers],
            "eliminated": [],
            "allActive": {all_names.get(t, str(t)): True for t in state["active"]},
        }

        if active_losers:
            min_cats = min(l["catWins"] for l in active_losers)
            for t in [l for l in active_losers if l["catWins"] == min_cats]:
                if t["teamId"] in state["active"]:
                    state["active"].remove(t["teamId"])
                e = {"teamId": t["teamId"], "team": t["team"], "week": week,
                     "catWins": t["catWins"], "round": state["currentRound"]}
                state["eliminated"].append(e)
                week_entry["eliminated"].append(e)
            week_entry["allActive"] = {
                all_names.get(t, str(t)): t in state["active"] for t in team_map
            }
            if len(state["active"]) <= 1:
                if len(state["active"]) == 1:
                    wid = state["active"][0]
                    wname = all_names.get(wid, str(wid))
                    state["champions"].append({
                        "teamId": wid, "team": wname,
                        "week": week, "round": state["currentRound"],
                    })
                    week_entry["champion"] = wname
                state["active"] = list(team_map.keys())
                state["currentRound"] += 1

        state["processedWeeks"].append(week)
        state["history"].append(week_entry)

    state["updated"] = updated
    return state


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

    matchup_period = getattr(league, 'currentMatchupPeriod',
                     getattr(league, 'current_matchup_period', current_week))
    print(f"  📅  currentMatchupPeriod: {matchup_period}")

    def has_scores(boxes):
        return any(
            (getattr(b, 'home_wins', 0) or 0) + (getattr(b, 'away_wins', 0) or 0) > 0
            for b in (boxes or [])
        )

    def get_active_week(league, week):
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
            if boxes_prev and any(getattr(b, 'home_team', None) for b in boxes_prev):
                print(f"  ⚠️  No scores yet — using week {week-1} (pre-scoring)")
                return week - 1, boxes_prev
        print(f"  ℹ️   Using week {week} (no prior week or no data)")
        return week, boxes_w

    scoring_week, prefetched_boxes = get_active_week(league, matchup_period)
    print(f"  📅  Scoring week resolved to: {scoring_week}")

    # ── Team map ──────────────────────────────────────────────────────────────
    team_map = {}
    for t in league.teams:
        raw_owners = getattr(t, "owners", []) or []
        owners = [o.get("displayName", o.get("firstName","?")) if isinstance(o, dict) else str(o)
                  for o in raw_owners]
        team_map[t.team_id] = {
            "id":     t.team_id,
            "name":   t.team_name,
            "abbrev": getattr(t, "team_abbrev", t.team_name[:3].upper()),
            "owners": owners,
            "logo":   getattr(t, "logo_url", getattr(t, "logo", "")),
            "rbName": espn_to_rb(t.team_name) or "",
        }

    # ── Standings (preliminary; enriched after allWeeks built) ────────────────
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
            "catWins":       0,
            "catLosses":     0,
            "catTies":       0,
            "allPlayPct":    None,
        })
    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1

    # ── Matchups ──────────────────────────────────────────────────────────────
    all_weeks_data = {}
    all_id_to_name = {t.team_id: t.team_name for t in league.teams}
    matchups_out = []

    try:
        req = league.espn_request

        STAT_KEYS = {
            "20":"R","21":"RBI","5":"HR","23":"SB","27":"Kbat",
            "2":"AVG","18":"OPS","34":"IP","37":"H","48":"K",
            "63":"QS","47":"ERA","41":"WHIP","60":"SVHD","57":"SV","83":"HLD",
        }
        LOWER_CATS = {"ERA","WHIP","H","Kbat"}
        id_to_name = {t.team_id: t.team_name for t in league.teams}

        def api_get(views, extra_params=None):
            import requests as _req
            if isinstance(views, str):
                views = [views]
            params_list = [('view', v) for v in views]
            if extra_params:
                for k, v2 in extra_params.items():
                    params_list.append((k, v2))
            cookies = getattr(req, 'cookies', {}) or {}
            headers = dict(getattr(req, 'headers', {}) or {})
            base = (f'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb'
                    f'/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}')
            resp = _req.get(base, params=params_list, cookies=cookies,
                            headers=headers, timeout=20)
            if resp.status_code != 200:
                base2 = (f'https://fantasy.espn.com/apis/v3/games/flb'
                         f'/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}')
                resp = _req.get(base2, params=params_list, cookies=cookies,
                                headers=headers, timeout=20)
            print(f"    HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.json()

        def fmt_v(v, lbl):
            if v is None: return "—"
            try:
                f = float(v)
                if lbl in {"AVG","OPS"}: return f"{f:.3f}"
                if lbl in {"ERA","WHIP"}: return f"{f:.2f}"
                if lbl == "IP": return f"{f:.1f}"
                return str(int(f)) if f == int(f) else str(round(f,1))
            except: return str(v)

        for sp in [1, 2]:
            try:
                data = api_get(['mScoreboard'], {'scoringPeriodId': sp})
                if not isinstance(data, dict):
                    continue

                schedule = data.get('schedule', [])
                all_periods = sorted(set(m.get('matchupPeriodId', 0) for m in schedule[:30]))
                print(f"  sp={sp}: {len(schedule)} entries, matchupPeriodIds={all_periods}")

                if schedule:
                    first = schedule[0]
                    print(f"  First entry keys: {list(first.keys())}")
                    print(f"  First entry matchupPeriodId={first.get('matchupPeriodId')} "
                          f"id={first.get('id')} "
                          f"home.teamId={first.get('home',{}).get('teamId')} "
                          f"away.teamId={first.get('away',{}).get('teamId')}")
                    home_cum = first.get('home',{}).get('cumulativeScore',{})
                    print(f"  First home cumulativeScore keys: {list(home_cum.keys())}")
                    print(f"  First home wins={home_cum.get('wins')} losses={home_cum.get('losses')}")

                MATCHUPS_PER_WEEK = 6
                by_period = {}
                for m in schedule:
                    if not m.get('home',{}).get('teamId') or not m.get('away',{}).get('teamId'):
                        continue
                    mid = m.get('id', 1)
                    pid = ((mid - 1) // MATCHUPS_PER_WEEK) + 1
                    by_period.setdefault(pid, []).append(m)
                print(f"  by_period weeks: {sorted(by_period.keys())} ({len(by_period)} weeks)")
                all_weeks_data = by_period
                all_id_to_name = id_to_name
                print(f"  Periods found: {sorted(by_period.keys())}")

                # FIX: Find current week = first week with ANY undecided matchup.
                # Skip fully-decided weeks (those are complete/historical).
                current_sp = None
                for pid in sorted(by_period.keys()):
                    week_matches = by_period[pid]
                    if any(m.get('winner') == 'UNDECIDED' for m in week_matches):
                        current_sp = pid
                        break
                if current_sp is None:
                    current_sp = max(by_period.keys())
                print(f"  Detected current fantasy week: {current_sp}")

                period_matches = by_period.get(current_sp, [])
                print(f"  Current week: {current_sp}, matchups: {len(period_matches)}")

                if not period_matches:
                    continue

                # FIX: fetch mBoxscore for the CURRENT FANTASY WEEK, not the ESPN period
                cat_data = {}
                try:
                    box_data = api_get(['mBoxscore'], {'scoringPeriodId': current_sp})
                    if isinstance(box_data, dict):
                        for m in box_data.get('schedule', []):
                            mid = m.get('id')
                            if mid is None:
                                continue
                            mp = m.get('matchupPeriodId')
                            derived_week = ((mid - 1) // MATCHUPS_PER_WEEK) + 1
                            # Accept if matchupPeriodId matches or if derived week matches
                            if mp == current_sp or (mp is None and derived_week == current_sp):
                                cat_data[mid] = m
                        print(f"  mBoxscore: {len(cat_data)} matchup details")
                except Exception as e:
                    print(f"  mBoxscore failed (ok): {e}")

                for m in period_matches:
                    home_d = m.get('home', {})
                    away_d = m.get('away', {})
                    if not home_d.get('teamId') or not away_d.get('teamId'):
                        continue

                    def parse_side(side, box_side=None):
                        tid   = side.get('teamId')
                        tname = id_to_name.get(tid, f'Team {tid}')
                        tm    = team_map.get(tid, {})
                        cum   = side.get('cumulativeScore', {})
                        cat_wins   = int(cum.get('wins',   0) or 0)
                        cat_losses = int(cum.get('losses', 0) or 0)

                        sbs = cum.get('scoreByStat', {})
                        if sbs:
                            non_zero = {str(k): round(float(v.get('score',0) or 0), 3)
                                        for k, v in sbs.items()
                                        if isinstance(v, dict) and (v.get('score') or 0) != 0}
                            print(f"      [{tname}] statIds: {non_zero}")

                        sbs_any = {str(k): v for k, v in sbs.items()}
                        stats = {}
                        stats['SVHD'] = extract_svhd(sbs_any, tname)

                        for stat_id, info in sbs_any.items():
                            lbl = STAT_KEYS.get(stat_id)
                            if lbl and lbl not in ('SV', 'HLD', 'SVHD'):
                                if isinstance(info, dict):
                                    v = info.get('score', info.get('value'))
                                    if v is not None:
                                        try: stats[lbl] = float(v)
                                        except: pass

                        if len(stats) <= 1 and box_side:
                            for stat_id, info in box_side.get('cumulativeScore', {}).get('scoreByStat', {}).items():
                                lbl = STAT_KEYS.get(str(stat_id))
                                if lbl and lbl not in ('SV', 'HLD', 'SVHD'):
                                    v = info.get('score', info.get('value'))
                                    if v is not None:
                                        try: stats[lbl] = float(v)
                                        except: pass

                        stats.pop('SV', None)
                        stats.pop('HLD', None)

                        if 'IP' in stats:
                            outs = int(round(stats['IP']))
                            stats['IP'] = float(f"{outs // 3}.{outs % 3}")

                        return {
                            'teamId': tid, 'team': tname,
                            'abbrev': tm.get('abbrev',''), 'rbName': tm.get('rbName',''),
                            'catWins': cat_wins, 'catLoss': cat_losses, 'catTies': 0,
                            'stats': stats,
                        }

                    mid = m.get('id')
                    box_m = cat_data.get(mid, {})
                    hs = parse_side(home_d, box_m.get('home'))
                    as_ = parse_side(away_d, box_m.get('away'))

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
                    matchups_out.append({
                        'home': hs, 'away': as_,
                        'leader': hs['team'] if hw else (as_['team'] if aw else 'Tied'),
                        'winner': 'UNDECIDED',
                    })
                    print(f"    ✓ {hs['team']} ({hs['catWins']}) vs {as_['team']} ({as_['catWins']})")

                if matchups_out:
                    # FIX: use current_sp (derived fantasy week), not sp (ESPN period number)
                    scoring_week = current_sp
                    break

            except Exception as e:
                import traceback
                print(f"  ⚠️  sp={sp} error: {e}")
                traceback.print_exc()

        if not matchups_out:
            print("  ⚠️  All matchup attempts failed — saving empty matchups.json")

    except Exception as e:
        import traceback
        print(f"  ⚠️  Matchup block failed: {e}")
        traceback.print_exc()

    # ── Build allWeeks output ──────────────────────────────────────────────────
    all_weeks_out = {str(scoring_week): matchups_out}
    try:
        for wk, wk_matches in all_weeks_data.items():
            if str(wk) == str(scoring_week):
                continue
            wk_list = []
            for m in wk_matches:
                home_d = m.get('home', {})
                away_d = m.get('away', {})
                hid = home_d.get('teamId')
                aid = away_d.get('teamId')
                if not hid or not aid:
                    continue
                h_tm = team_map.get(hid, {})
                a_tm = team_map.get(aid, {})

                def parse_wk_side(side_d, tm):
                    tid = side_d.get('teamId')
                    cum = side_d.get('cumulativeScore', {})
                    cat_wins   = int(cum.get('wins',   0) or 0)
                    cat_losses = int(cum.get('losses', 0) or 0)
                    stats = {}
                    SKEYS = {
                        "20":"R","5":"HR","21":"RBI","27":"Kbat","23":"SB",
                        "2":"AVG","18":"OPS","34":"IP","37":"H","48":"K",
                        "63":"QS","47":"ERA","41":"WHIP","60":"SVHD","57":"SV","83":"HLD",
                    }
                    raw_sbs = cum.get('scoreByStat', {})
                    sbs_norm = {str(k): v for k, v in raw_sbs.items()}
                    tname_local = all_id_to_name.get(tid, f'Team {tid}')
                    stats['SVHD'] = extract_svhd(sbs_norm, tname_local)
                    for sid, info in sbs_norm.items():
                        lbl = SKEYS.get(sid)
                        if lbl and lbl not in ('SV', 'HLD', 'SVHD') and isinstance(info, dict):
                            v = info.get('score', info.get('value'))
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if lbl == 'IP':
                                        outs = int(round(fv))
                                        fv = float(f"{outs//3}.{outs%3}")
                                    stats[lbl] = fv
                                except: pass
                    stats.pop('SV', None)
                    stats.pop('HLD', None)
                    return {
                        'teamId': tid,
                        'team': all_id_to_name.get(tid, f'Team {tid}'),
                        'abbrev': tm.get('abbrev', ''),
                        'rbName': tm.get('rbName', ''),
                        'catWins': cat_wins, 'catLoss': cat_losses, 'catTies': 0,
                        'categories': {},
                        'stats': stats,
                    }

                hs = parse_wk_side(home_d, h_tm)
                as_ = parse_wk_side(away_d, a_tm)

                LOWER = {"ERA","WHIP","H","Kbat"}
                hc, ac = {}, {}
                for lbl in set(list(hs['stats']) + list(as_['stats'])):
                    hv = hs['stats'].get(lbl)
                    av = as_['stats'].get(lbl)
                    if hv is None or av is None: continue
                    lower = lbl in LOWER
                    def fmtv(v, l):
                        if v is None: return '—'
                        try:
                            f = float(v)
                            if l in {'AVG','OPS'}: return f'{f:.3f}'
                            if l in {'ERA','WHIP'}: return f'{f:.2f}'
                            if l == 'IP': return f'{f:.1f}'
                            return str(int(f)) if f == int(f) else str(round(f,1))
                        except: return str(v)
                    if abs(hv - av) < 0.0001: hr = ar = 'TIE'
                    elif (hv < av) == lower: hr, ar = 'WIN', 'LOSS'
                    else: hr, ar = 'LOSS', 'WIN'
                    hc[lbl] = {'value': fmtv(hv, lbl), 'result': hr}
                    ac[lbl] = {'value': fmtv(av, lbl), 'result': ar}

                ties_count = sum(1 for v in hc.values() if v['result'] == 'TIE')
                hs['categories'] = hc; hs['catTies'] = ties_count; del hs['stats']
                as_['categories'] = ac; as_['catTies'] = ties_count; del as_['stats']
                hw = hs['catWins'] > as_['catWins']
                aw = as_['catWins'] > hs['catWins']
                winner = m.get('winner', 'UNDECIDED')
                wk_list.append({
                    'home': hs, 'away': as_,
                    'leader': hs['team'] if hw else (as_['team'] if aw else 'Tied'),
                    'winner': winner,
                })

            if wk_list:
                all_weeks_out[str(wk)] = wk_list

        print(f"  ✅  allWeeks: {sorted(int(k) for k in all_weeks_out.keys())} weeks saved")
    except Exception as e:
        import traceback
        print(f"  ⚠️  allWeeks build: {e}")
        traceback.print_exc()

    # ── Enrich standings with live 2026 data ──────────────────────────────────
    try:
        # Sum cat wins/losses across completed matchups
        team_cat_totals = defaultdict(lambda: {'catWins': 0, 'catLosses': 0, 'catTies': 0})
        for wk_str, wk_list in all_weeks_out.items():
            for m in wk_list:
                if m.get('winner') == 'UNDECIDED':
                    continue
                for side_key in ['home', 'away']:
                    side = m[side_key]
                    tid = side.get('teamId')
                    if not tid:
                        continue
                    team_cat_totals[tid]['catWins']   += side.get('catWins', 0)
                    team_cat_totals[tid]['catLosses'] += side.get('catLoss', 0)
                    team_cat_totals[tid]['catTies']   += side.get('catTies', 0)

        # Streak from allWeeks in chronological order
        team_streaks = {}
        for wk in sorted(int(w) for w in all_weeks_out.keys()):
            for m in all_weeks_out.get(str(wk), []):
                if m.get('winner') == 'UNDECIDED':
                    continue
                for side_key in ['home', 'away']:
                    side = m[side_key]
                    opp_key = 'away' if side_key == 'home' else 'home'
                    tid = side.get('teamId')
                    if not tid:
                        continue
                    my_w = side.get('catWins', 0)
                    op_w = m[opp_key].get('catWins', 0)
                    result = 'WIN' if my_w > op_w else 'LOSS' if op_w > my_w else 'TIE'
                    if tid not in team_streaks:
                        team_streaks[tid] = {'type': result, 'length': 1}
                    elif team_streaks[tid]['type'] == result:
                        team_streaks[tid]['length'] += 1
                    else:
                        team_streaks[tid] = {'type': result, 'length': 1}

        # All-play percentage
        ap_totals = compute_allplay(all_weeks_out)

        for s in standings:
            tid = s['id']
            ct = team_cat_totals.get(tid, {})
            s['catWins']   = ct.get('catWins',   0)
            s['catLosses'] = ct.get('catLosses', 0)
            s['catTies']   = ct.get('catTies',   0)
            sk = team_streaks.get(tid)
            if sk:
                s['streak']     = sk['length']
                s['streakType'] = sk['type']
            ap = ap_totals.get(tid)
            if ap:
                tot = ap['w'] + ap['l'] + ap['t']
                s['allPlayPct'] = round((ap['w'] + ap['t'] * 0.5) / tot, 4) if tot else None
            else:
                s['allPlayPct'] = None

        print(f"  ✅  Standings enriched: cat wins, streak, all-play")
    except Exception as e:
        import traceback
        print(f"  ⚠️  Standings enrichment failed: {e}")
        traceback.print_exc()

    save("standings.json", {"week": scoring_week, "standings": standings, "updated": updated})

    # ── Projections ───────────────────────────────────────────────────────────
    team_projections = {}
    try:
        proj_params = [('view', 'mRoster'), ('view', 'mSettings'),
                       ('scoringPeriodId', scoring_week)]
        cookies = getattr(req, 'cookies', {}) or {}
        headers = dict(getattr(req, 'headers', {}) or {})
        import requests as _req2
        base = (f'https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb'
                f'/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}')
        resp = _req2.get(base, params=proj_params, cookies=cookies,
                         headers=headers, timeout=20)
        print(f"  Projections HTTP {resp.status_code}")
        if resp.status_code == 200:
            proj_data = resp.json()
            PROJ_STAT_KEYS = {
                "20":"R","5":"HR","21":"RBI","27":"Kbat","23":"SB",
                "2":"AVG","18":"OPS","34":"IP","37":"H","48":"K",
                "63":"QS","47":"ERA","41":"WHIP","60":"SVHD",
            }
            for team_entry in proj_data.get('teams', []):
                tid = team_entry.get('id')
                if not tid: continue
                tm = team_map.get(tid, {})
                proj_stats = {
                    "R":0,"HR":0,"RBI":0,"Kbat":0,"SB":0,
                    "AVG":0,"OPS":0,"IP":0,"H":0,"K":0,
                    "QS":0,"ERA":0,"WHIP":0,"SVHD":0,
                }
                roster = team_entry.get('roster', {}).get('entries', [])
                for entry in roster:
                    slot = entry.get('lineupSlotId', 16)
                    if slot in (16, 17, 18, 19, 20):
                        continue
                    player_pool = entry.get('playerPoolEntry', {})
                    player_stats = player_pool.get('player', {}).get('stats', [])
                    for stat_entry in player_stats:
                        if stat_entry.get('statSplitTypeId') == 5:
                            sbs = stat_entry.get('stats', {})
                            for sid, val in sbs.items():
                                lbl = PROJ_STAT_KEYS.get(str(sid))
                                if lbl and val:
                                    try:
                                        v = float(val)
                                        if lbl not in ('AVG','OPS','ERA','WHIP'):
                                            proj_stats[lbl] = proj_stats.get(lbl, 0) + v
                                    except: pass
                team_projections[str(tid)] = {
                    'name': tm.get('name',''),
                    'rbName': tm.get('rbName',''),
                    'stats': proj_stats,
                }
            print(f"  Projections: {len(team_projections)} teams")
    except Exception as e:
        print(f"  ⚠️  Projections failed: {e}")

    save("matchups.json", {
        "week":        scoring_week,
        "matchups":    matchups_out,
        "allWeeks":    all_weeks_out,
        "projections": team_projections,
        "updated":     updated,
    })

    # ── Category W-L 2026 ─────────────────────────────────────────────────────
    try:
        cat_wl = compute_cat_wl(all_weeks_out)
        save("cat_wl_2026.json", {
            "season": SEASON,
            "teamCatWL": {name: dict(cats) for name, cats in cat_wl.items()},
            "updated": updated,
        })
    except Exception as e:
        print(f"  ⚠️  Cat W-L 2026 failed: {e}")

    # ── History matchups for 2026 ─────────────────────────────────────────────
    try:
        history_matchups = []
        for wk_str, wk_list in all_weeks_out.items():
            wk = int(wk_str)
            for m in wk_list:
                if m.get('winner') == 'UNDECIDED':
                    continue
                hw = m['home'].get('catWins', 0)
                aw = m['away'].get('catWins', 0)
                winner = (m['home']['team'] if hw > aw
                          else m['away']['team'] if aw > hw else 'TIE')
                history_matchups.append({
                    'season': SEASON, 'week': wk,
                    'home': m['home']['team'],
                    'away': m['away']['team'],
                    'homeCatW': hw, 'awayCatW': aw,
                    'winner': winner,
                })
        save("history_matchups.json", {'matchups': history_matchups})
    except Exception as e:
        print(f"  ⚠️  History matchups failed: {e}")

    # ── Team stats (computed from allWeeks — espn-api t.stats is empty for categories leagues) ──
    season_stats_by_id = compute_season_stats(all_weeks_out)
    team_stats = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        # Start with any espn-api stats (usually empty for categories leagues)
        stats = {}
        raw_stats = getattr(t, "stats", {}) or getattr(t, "valuesByStat", {}) or {}
        for k, v in raw_stats.items():
            lbl = STAT_MAP.get(str(k))
            if lbl and v is not None:
                try:
                    stats[lbl] = round(float(v), 3)
                except (TypeError, ValueError):
                    pass
        # Override / fill in with allWeeks-derived season totals (more reliable)
        stats.update(season_stats_by_id.get(t.team_id, {}))
        team_stats.append({**tm, "wins": s_rec.get("wins",0),
                           "losses": s_rec.get("losses",0), "stats": stats})
    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── Rosters ───────────────────────────────────────────────────────────────
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

    # ── Power Rankings ────────────────────────────────────────────────────────
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

    # ── Meta ──────────────────────────────────────────────────────────────────
    save("meta.json", {
        "leagueName":  getattr(league.settings, "name", "The League"),
        "season":      SEASON,
        "currentWeek": scoring_week,
        "teamCount":   len(league.teams),
        "teams":       [{"id":t["id"],"name":t["name"],"abbrev":t["abbrev"]} for t in standings],
        "updated":     updated,
    })

    # ── KOH (no box_scores — uses schedule data) ──────────────────────────────
    save("koh.json", update_koh_from_schedule(
        all_weeks_out, team_map, all_id_to_name, updated
    ))

    # ── Draft + Keepers ───────────────────────────────────────────────────────
    draft_picks, keeper_by_name = fetch_draft_and_keepers(league, team_map)
    save("draft.json", {"season": SEASON, "picks": draft_picks,
                        "keeperEligible": keeper_by_name, "updated": updated})

    print(f"\n🏆  Done! Week {scoring_week}.")


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
