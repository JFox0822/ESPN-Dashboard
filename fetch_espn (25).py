#!/usr/bin/env python3
"""
ESPN Fantasy Baseball – Data Fetcher (espn-api version)
Uses the espn-api library for robust auth handling.
"""

import json
import os
import sys
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
LEAGUE_ID = int(os.environ.get("ESPN_LEAGUE_ID", "163020"))
ESPN_S2   = os.environ.get("ESPN_S2", "")
ESPN_SWID = os.environ.get("ESPN_SWID", "")
SEASON    = 2026

STAT_MAP = {
    "20": "R", "21": "RBI", "5": "HR", "23": "SB", "27": "Kbat",
    "2": "AVG", "17": "OPS", "34": "IP", "41": "H", "48": "K",
    "63": "QS", "47": "ERA", "53": "WHIP", "57": "SV", "83": "HLD",
}

SLOT_MAP = {
    0:"C", 1:"1B", 2:"2B", 3:"3B", 4:"SS", 5:"OF", 6:"2B/SS",
    7:"1B/3B", 8:"LF", 9:"CF", 10:"RF", 11:"DH", 12:"UTIL",
    13:"SP", 14:"RP", 15:"P", 16:"BE", 17:"IL", 18:"IL10",
    19:"IL60", 20:"NA", 21:"BE", 22:"IL",
}

POS_MAP = {
    1:"C", 2:"1B", 3:"2B", 4:"3B", 5:"SS", 6:"OF",
    7:"2B/SS", 8:"1B/3B", 9:"P", 10:"SP", 11:"RP", 12:"DH",
}

LOWER_BETTER = {"ERA", "WHIP", "H"}

# ── Helpers ────────────────────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def save(filename, obj):
    os.makedirs("data", exist_ok=True)
    path = f"data/{filename}"
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✅  {path}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    try:
        from espn_api.baseball import League
    except ImportError:
        print("❌  espn-api not installed. Run: pip install espn-api", file=sys.stderr)
        sys.exit(1)

    print(f"🔄  Connecting to ESPN league {LEAGUE_ID}, season {SEASON} …")

    # espn-api handles cookie auth internally
    kwargs = {"league_id": LEAGUE_ID, "year": SEASON}
    if ESPN_S2 and ESPN_SWID:
        kwargs["espn_s2"] = ESPN_S2
        kwargs["swid"]    = ESPN_SWID
        print("  🔐  Using ESPN_S2 + SWID cookies")
    else:
        print("  ⚠️   No cookies — fetching as public league")

    try:
        league = League(**kwargs)
    except Exception as e:
        print(f"❌  Failed to connect: {e}", file=sys.stderr)
        sys.exit(1)

    updated = now_utc()
    current_week = league.current_week
    print(f"  📅  Current week: {current_week}")

    # ── 1. Team map ─────────────────────────────────────────────────────────────
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
        }

    # ── 2. Standings ─────────────────────────────────────────────────────────────
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
            "seed":          getattr(t, "playoff_pct", 0),
        })

    standings.sort(key=lambda x: (-x["wins"], x["losses"], -x["pointsFor"]))
    for i, s in enumerate(standings):
        s["rank"] = i + 1

    save("standings.json", {"week": current_week, "standings": standings, "updated": updated})

    # ── 3. Matchups ──────────────────────────────────────────────────────────────
    matchups_out = []
    try:
        box_scores = league.box_scores(current_week)
        for b in box_scores:
            def parse_side(home_team, cat_wins, cat_losses, cat_ties):
                if home_team is None:
                    return {"teamId": None, "team": "BYE", "abbrev": "BYE",
                            "catWins": 0, "catLoss": 0, "catTies": 0, "categories": {}}
                tm = team_map.get(home_team.team_id, {})
                return {
                    "teamId":   home_team.team_id,
                    "team":     home_team.team_name,
                    "abbrev":   tm.get("abbrev", ""),
                    "catWins":  cat_wins,
                    "catLoss":  cat_losses,
                    "catTies":  cat_ties,
                    "categories": {},
                }
            home_side = parse_side(b.home_team, b.home_wins, b.home_losses, b.home_ties)
            away_side = parse_side(b.away_team, b.away_wins, b.away_losses, b.away_ties)
            hw = home_side["catWins"] > away_side["catWins"]
            aw = away_side["catWins"] > home_side["catWins"]
            matchups_out.append({
                "home":   home_side,
                "away":   away_side,
                "leader": home_side["team"] if hw else (away_side["team"] if aw else "Tied"),
                "winner": "UNDECIDED",
            })
    except Exception as e:
        print(f"  ⚠️   Box scores unavailable: {e}", file=sys.stderr)

    save("matchups.json", {
        "week":     current_week,
        "period":   current_week,
        "matchups": matchups_out,
        "updated":  updated,
    })

    # ── 4. Team stats ────────────────────────────────────────────────────────────
    team_stats = []
    for t in league.teams:
        tm   = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        stats = {}
        raw_stats = getattr(t, "stats", {}) or getattr(t, "valuesByStat", {}) or {}
        for k, v in raw_stats.items():
            lbl = STAT_MAP.get(str(k))
            if lbl:
                stats[lbl] = round(v, 3) if isinstance(v, float) else v
        team_stats.append({
            **tm,
            "wins":   s_rec.get("wins", 0),
            "losses": s_rec.get("losses", 0),
            "stats":  stats,
        })
    team_stats.sort(key=lambda x: (-x["wins"], x["losses"]))
    save("team_stats.json", {"season": SEASON, "teams": team_stats, "updated": updated})

    # ── 5. Rosters ───────────────────────────────────────────────────────────────
    rosters_out = []
    for t in league.teams:
        tm = team_map[t.team_id]
        players = []
        for p in (t.roster or []):
            slot_id = getattr(p, "lineupSlot", getattr(p, "slot_id", 16))
            slot_label = SLOT_MAP.get(slot_id, "BE") if isinstance(slot_id, int) else str(slot_id)

            pos_id = getattr(p, "defaultPositionId", getattr(p, "position_id", 0)) or 0
            primary_pos = POS_MAP.get(pos_id, "?")

            eligible_ids = getattr(p, "eligibleSlots", []) or []
            eligible_pos = list(dict.fromkeys(
                SLOT_MAP.get(s, "") for s in eligible_ids
                if SLOT_MAP.get(s, "") not in ("BE","IL","IL10","IL60","NA","")
            ))
            eligible_str = "/".join(eligible_pos)[:20]

            is_pitcher = pos_id in (9, 10, 11, 13)
            inj_status = getattr(p, "injuryStatus", "ACTIVE") or "ACTIVE"

            stats = {}
            for k, v in (getattr(p, "stats", {}) or {}).items():
                lbl = STAT_MAP.get(str(k))
                if lbl:
                    stats[lbl] = round(v, 3) if isinstance(v, float) else v

            acq_type  = (getattr(p, "acquisitionType", "") or "").upper()
            draft_round = getattr(p, "draftRound", 0) or 0
            # keeper eligible: drafted in round 13+ OR acquired as FA/waiver (not via draft)
            is_keeper_elig = (draft_round >= 13) or (draft_round == 0 and any(
                x in acq_type for x in ["FREE", "WAIVER", "FA"]
            ))

            players.append({
                "name":          getattr(p, "name", "Unknown"),
                "slot":          slot_label,
                "position":      primary_pos,
                "eligible":      eligible_str,
                "isPitcher":     is_pitcher,
                "injStatus":     inj_status,
                "stats":         stats,
                "tier":          "",
                "acquisitionType": acq_type,
                "draftRound":    draft_round,
                "keeperEligible": is_keeper_elig,
            })

        rosters_out.append({**tm, "teamId": t.team_id, "players": players})

    save("rosters.json", {
        "season":    SEASON,
        "week":      current_week,
        "teams":     rosters_out,
        "projBlend": {"projPct": 0, "actualPct": 100},
        "updated":   updated,
    })

    # ── 6. Power Rankings ────────────────────────────────────────────────────────
    ts_map = {t["id"]: t.get("stats", {}) for t in team_stats}
    power = []
    for t in league.teams:
        tm    = team_map[t.team_id]
        s_rec = next((s for s in standings if s["id"] == t.team_id), {})
        power.append({
            "id":          t.team_id,
            "name":        t.team_name,
            "abbrev":      tm.get("abbrev", ""),
            "overallW":    s_rec.get("wins", 0),
            "overallL":    s_rec.get("losses", 0),
            "overallRank": s_rec.get("rank", 0),
            "h2hWins":     s_rec.get("wins", 0),
            "h2hLosses":   s_rec.get("losses", 0),
            "pwWins":      0,
            "pwLosses":    0,
            "composite":   0.0,
            "catRanks":    {},
        })

    # Compute category ranks
    all_cats = ["R","RBI","HR","SB","AVG","OPS","IP","H","K","QS","ERA","WHIP","SV","HLD"]
    for cat in all_cats:
        vals = [(p["id"], ts_map.get(p["id"], {}).get(cat)) for p in power]
        vals = [(pid, v) for pid, v in vals if v is not None]
        if len(vals) < 2:
            continue
        vals.sort(key=lambda x: x[1], reverse=(cat not in LOWER_BETTER))
        for rank, (pid, _) in enumerate(vals, 1):
            for p in power:
                if p["id"] == pid:
                    p["catRanks"][cat] = rank

    power.sort(key=lambda x: (-x["overallW"], x["overallL"]))
    for i, p in enumerate(power):
        p["pwRank"]    = i + 1
        p["rankDelta"] = 0

    save("power_rankings.json", {"week": current_week, "rankings": power, "updated": updated})

    # ── 7. Meta ──────────────────────────────────────────────────────────────────
    team_list = [{"id": t["id"], "name": t["name"], "abbrev": t["abbrev"]} for t in standings]
    save("meta.json", {
        "leagueName":    getattr(league.settings, "name", "The League"),
        "season":        SEASON,
        "currentWeek":   current_week,
        "scoringPeriod": current_week,
        "teamCount":     len(league.teams),
        "teams":         team_list,
        "updated":       updated,
    })

    # ── 8. KOH ─────────────────────────────────────────────────────────────────
    koh_state = update_koh(league, current_week, team_map, updated)
    save("koh.json", koh_state)

    # ── 9. Draft + Keepers ───────────────────────────────────────────────────
    draft_picks, keeper_by_name = fetch_draft_and_keepers(league, team_map)
    save("draft.json", {
        "season":          SEASON,
        "picks":           draft_picks,
        "keeperEligible":  keeper_by_name,
        "updated":         updated,
    })

    print(f"\n🏆  Done! Week {current_week}.")


# ── KOH (King of the Hill) ─────────────────────────────────────────────────
def update_koh(league, current_week, team_map, updated):
    import os
    koh_path = "data/koh.json"
    all_names = {tid: tm["name"] for tid, tm in team_map.items()}

    if os.path.exists(koh_path):
        with open(koh_path) as f:
            state = json.load(f)
        # Reset if new season
        if state.get("season") != SEASON:
            state = None
    else:
        state = None

    if state is None:
        state = {
            "season": SEASON,
            "active": list(team_map.keys()),  # list of team IDs
            "eliminated": [],
            "champions": [],
            "currentRound": 1,
            "processedWeeks": [],
            "history": [],
            "teamNames": all_names,
        }

    # Process all completed weeks not yet processed
    for week in range(1, current_week):
        if week in state["processedWeeks"]:
            continue
        try:
            boxes = league.box_scores(week)
        except Exception as e:
            print(f"  ⚠️  KOH: could not get week {week} box scores: {e}")
            continue

        all_scores = {}
        losers = []
        for b in boxes:
            ht = b.home_team
            at = b.away_team
            if not ht or not at:
                continue
            hid, aid = ht.team_id, at.team_id
            hw = getattr(b, 'home_wins', 0) or 0
            aw = getattr(b, 'away_wins', 0) or 0
            all_scores[hid] = hw
            all_scores[aid] = aw
            if hw < aw:
                losers.append({"teamId": hid, "team": all_names.get(hid, f"Team {hid}"), "catWins": hw})
            elif aw < hw:
                losers.append({"teamId": aid, "team": all_names.get(aid, f"Team {aid}"), "catWins": aw})
            # ties: no loser for KOH purposes

        # Among losers, only eliminate active teams
        active_set = set(state["active"])
        active_losers = [l for l in losers if l["teamId"] in active_set]

        week_entry = {
            "week": week,
            "round": state["currentRound"],
            "allScores": {str(k): v for k, v in all_scores.items()},
            "losers": [{"team": l["team"], "catWins": l["catWins"]} for l in losers],
            "eliminated": [],
            "allActive": {all_names.get(tid, str(tid)): True for tid in state["active"]},
        }

        if active_losers:
            min_cats = min(l["catWins"] for l in active_losers)
            to_elim = [l for l in active_losers if l["catWins"] == min_cats]
            for t in to_elim:
                if t["teamId"] in state["active"]:
                    state["active"].remove(t["teamId"])
                e_entry = {
                    "teamId": t["teamId"],
                    "team": t["team"],
                    "week": week,
                    "catWins": t["catWins"],
                    "round": state["currentRound"],
                }
                state["eliminated"].append(e_entry)
                week_entry["eliminated"].append(e_entry)

            # Update allActive after elimination
            week_entry["allActive"] = {all_names.get(tid, str(tid)): tid in state["active"] for tid in team_map}

            # Check for champion
            if len(state["active"]) == 1:
                winner_id = state["active"][0]
                winner_name = all_names.get(winner_id, f"Team {winner_id}")
                state["champions"].append({
                    "teamId": winner_id,
                    "team": winner_name,
                    "week": week,
                    "round": state["currentRound"],
                })
                week_entry["champion"] = winner_name
                # Reset — all 12 start fresh
                state["active"] = list(team_map.keys())
                state["currentRound"] += 1
            elif len(state["active"]) == 0:
                # Everyone tied out — restart
                state["active"] = list(team_map.keys())
                state["currentRound"] += 1

        state["processedWeeks"].append(week)
        state["history"].append(week_entry)

    state["updated"] = updated
    return state


# ── Draft + Keepers ────────────────────────────────────────────────────────
def fetch_draft_and_keepers(league, team_map):
    draft_picks = []
    keeper_eligible = {}  # teamId -> [{name, source}]

    try:
        for pick in (league.draft or []):
            try:
                team_id = pick.team.team_id if hasattr(pick, 'team') and pick.team else None
                player_name = getattr(pick, 'playerName', None) or getattr(pick, 'player_name', 'Unknown')
                round_num = getattr(pick, 'round_num', 0) or 0
                round_pick = getattr(pick, 'round_pick', 0) or 0
                team_name = team_map.get(team_id, {}).get("name", "?") if team_id else "?"
                keeper_elig = round_num >= 13

                draft_picks.append({
                    "round": round_num,
                    "pick": round_pick,
                    "overall": (round_num - 1) * len(team_map) + round_pick,
                    "teamId": team_id,
                    "team": team_name,
                    "player": player_name,
                    "keeperEligible": keeper_elig,
                })
                if keeper_elig and team_id:
                    if team_id not in keeper_eligible:
                        keeper_eligible[team_id] = []
                    keeper_eligible[team_id].append({"name": player_name, "source": f"Rd {round_num}"})
            except Exception as pe:
                print(f"  ⚠️  Draft pick error: {pe}")
    except Exception as e:
        print(f"  ⚠️  Draft data unavailable: {e}")

    # FA pickups from current rosters
    try:
        for t in league.teams:
            for player in (t.roster or []):
                try:
                    acq = getattr(player, 'acquisitionType', '') or ''
                    if any(x in acq.upper() for x in ['FREE', 'WAIVER', 'FA']):
                        if t.team_id not in keeper_eligible:
                            keeper_eligible[t.team_id] = []
                        pname = getattr(player, 'name', 'Unknown')
                        # Avoid duplicates
                        if not any(p['name'] == pname for p in keeper_eligible[t.team_id]):
                            keeper_eligible[t.team_id].append({"name": pname, "source": "FA/Waiver"})
                except Exception:
                    pass
    except Exception as e:
        print(f"  ⚠️  FA acquisition data: {e}")

    # Convert teamId keys to team names for dashboard
    keeper_by_name = {}
    for tid, players in keeper_eligible.items():
        tname = team_map.get(tid, {}).get("name", f"Team {tid}")
        keeper_by_name[tname] = players

    return draft_picks, keeper_by_name

if __name__ == "__main__":
    main()
