import asyncio
import io
import logging
import os
import secrets
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import aiosqlite
import discord
import requests
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
from flask import Flask, redirect, render_template_string, request
from itsdangerous import URLSafeTimedSerializer
from werkzeug.middleware.proxy_fix import ProxyFix

load_dotenv()

TOKEN = os.getenv("TOKEN")
PREFIX = os.getenv("PREFIX", "!")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI", "")
WEB_URL = os.getenv("WEB_URL", "").rstrip("/")
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_hex(32))
DB_PATH = os.getenv("DB_PATH", "data/bot.db")
PORT = int(os.getenv("PORT") or os.getenv("WEB_PORT") or "8080")
API = "https://discord.com/api/v10"

DEFAULT, SUCCESS, ERROR, WARN, INFO, NEUTRAL = 0x5865F2, 0x57F287, 0xED4245, 0xFEE75C, 0x5865F2, 0x2B2D31

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("venice")

# ─────────────────────────── database ───────────────────────────

_conn: aiosqlite.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS guilds (
    guild_id INTEGER PRIMARY KEY, log_channel INTEGER, mod_log INTEGER, join_log INTEGER,
    verify_log INTEGER, welcome_channel INTEGER, goodbye_channel INTEGER,
    welcome_title TEXT, welcome_message TEXT, welcome_color INTEGER, welcome_image TEXT,
    goodbye_message TEXT, verify_enabled INTEGER DEFAULT 0, verify_role INTEGER,
    unverified_role INTEGER, staff_channel INTEGER, staff_panel INTEGER,
    signin_channel INTEGER, signin_word TEXT, signout_word TEXT,
    block_punish TEXT DEFAULT 'delete', block_punish_minutes INTEGER DEFAULT 10,
    antiraid_enabled INTEGER DEFAULT 1, antiraid_threshold INTEGER DEFAULT 8,
    antiraid_window INTEGER DEFAULT 12, antiraid_action TEXT DEFAULT 'alert',
    min_account_age INTEGER DEFAULT 0, automod_enabled INTEGER DEFAULT 1, automod_escalation INTEGER DEFAULT 3,
    voice_hub INTEGER, ticket_category INTEGER, ticket_log INTEGER, ticket_staff INTEGER,
    ticket_panel INTEGER, partner_channel INTEGER, partner_role INTEGER, invite_log INTEGER
);
CREATE TABLE IF NOT EXISTS blocked_words (guild_id INTEGER, word TEXT, PRIMARY KEY (guild_id, word));
CREATE TABLE IF NOT EXISTS ignored_channels (guild_id INTEGER, channel_id INTEGER, PRIMARY KEY (guild_id, channel_id));
CREATE TABLE IF NOT EXISTS warnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, user_id INTEGER,
    mod_id INTEGER, reason TEXT, created_at TEXT
);
CREATE TABLE IF NOT EXISTS verifications (
    guild_id INTEGER, user_id INTEGER, ip TEXT, user_agent TEXT,
    verified_at TEXT, PRIMARY KEY (guild_id, user_id)
);
CREATE TABLE IF NOT EXISTS alts (
    guild_id INTEGER, user_id INTEGER, matched_user_id INTEGER, reason TEXT, created_at TEXT
);
CREATE TABLE IF NOT EXISTS staff (
    guild_id INTEGER, user_id INTEGER, points INTEGER DEFAULT 0, days_present INTEGER DEFAULT 0,
    last_signin TEXT, on_duty INTEGER DEFAULT 0, duty_start TEXT, total_seconds INTEGER DEFAULT 0,
    active_seconds INTEGER DEFAULT 0, last_message TEXT, last_active TEXT,
    last_penalty TEXT, msg_count INTEGER DEFAULT 0, msg_date TEXT,
    PRIMARY KEY (guild_id, user_id)
);
CREATE TABLE IF NOT EXISTS staff_ranks (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, name TEXT,
    threshold INTEGER, role_id INTEGER, position INTEGER
);
CREATE TABLE IF NOT EXISTS duty (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, user_id INTEGER,
    start_ts TEXT, end_ts TEXT, seconds INTEGER
);
CREATE TABLE IF NOT EXISTS temp_voice (
    channel_id INTEGER PRIMARY KEY, guild_id INTEGER, owner_id INTEGER, expires_at TEXT, private INTEGER
);
CREATE TABLE IF NOT EXISTS tickets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, channel_id INTEGER, user_id INTEGER,
    type TEXT, reason TEXT, status TEXT DEFAULT 'open', claimed_by INTEGER, created_at TEXT
);
CREATE TABLE IF NOT EXISTS ticket_types (
    guild_id INTEGER, name TEXT, emoji TEXT, description TEXT, PRIMARY KEY (guild_id, name)
);
CREATE TABLE IF NOT EXISTS partners (
    guild_id INTEGER, user_id INTEGER, note TEXT, added_by INTEGER, created_at TEXT,
    PRIMARY KEY (guild_id, user_id)
);
CREATE TABLE IF NOT EXISTS invites (
    guild_id INTEGER, user_id INTEGER, joins INTEGER DEFAULT 0, leaves INTEGER DEFAULT 0,
    fake INTEGER DEFAULT 0, PRIMARY KEY (guild_id, user_id)
);
CREATE TABLE IF NOT EXISTS invite_joins (
    id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, user_id INTEGER,
    inviter_id INTEGER, created_at TEXT
);
"""

MIGRATIONS = {
    "guilds": {
        "signin_channel": "INTEGER", "signin_word": "TEXT", "signout_word": "TEXT",
        "block_punish": "TEXT DEFAULT 'delete'", "block_punish_minutes": "INTEGER DEFAULT 10",
        "voice_hub": "INTEGER", "ticket_category": "INTEGER", "ticket_log": "INTEGER",
        "ticket_staff": "INTEGER", "ticket_panel": "INTEGER", "partner_channel": "INTEGER",
        "partner_role": "INTEGER", "invite_log": "INTEGER",
    },
    "staff": {
        "active_seconds": "INTEGER DEFAULT 0", "last_message": "TEXT", "last_active": "TEXT",
        "last_penalty": "TEXT", "msg_count": "INTEGER DEFAULT 0", "msg_date": "TEXT",
    },
}


async def _raw(sql):
    c = await _conn.execute(sql)
    return await c.fetchall()


async def db_init():
    global _conn, DB_PATH
    try:
        os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
        probe = os.path.join(os.path.dirname(DB_PATH) or ".", ".probe")
        with open(probe, "w"):
            pass
        os.remove(probe)
    except OSError as e:
        log.warning("DB path %s unusable (%s) — falling back to data/bot.db", DB_PATH, e)
        DB_PATH = "data/bot.db"
        os.makedirs("data", exist_ok=True)
    _conn = await aiosqlite.connect(DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.executescript(SCHEMA)
    for table, cols in MIGRATIONS.items():
        existing = {r["name"] for r in await _raw(f"PRAGMA table_info({table})")}
        for col, decl in cols.items():
            if col not in existing:
                await _conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    await _conn.commit()
    log.info("Database ready at %s", DB_PATH)


async def db_close():
    if _conn:
        await _conn.close()
        log.info("Database closed")


async def ex(sql, p=()):
    c = await _conn.execute(sql, p)
    await _conn.commit()
    return c


async def one(sql, p=()):
    c = await _conn.execute(sql, p)
    return await c.fetchone()


async def all_(sql, p=()):
    c = await _conn.execute(sql, p)
    return await c.fetchall()


async def settings(gid: int) -> dict:
    r = await one("SELECT * FROM guilds WHERE guild_id=?", (gid,))
    if r is None:
        await ex("INSERT OR IGNORE INTO guilds (guild_id) VALUES (?)", (gid,))
        r = await one("SELECT * FROM guilds WHERE guild_id=?", (gid,))
    return dict(r)


async def setg(gid: int, **f):
    if not f:
        return
    await settings(gid)
    await ex(f"UPDATE guilds SET {', '.join(k + ' = ?' for k in f)} WHERE guild_id=?", (*f.values(), gid))


async def ensure_staff(gid: int, uid: int):
    r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (gid, uid))
    if r is None:
        await ex("INSERT INTO staff (guild_id, user_id) VALUES (?,?)", (gid, uid))
        r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (gid, uid))
    return r


# ─────────────────────────── embeds ───────────────────────────

def E(desc: str, color: int = DEFAULT, title: str | None = None) -> discord.Embed:
    e = discord.Embed(description=desc, color=color, timestamp=discord.utils.utcnow())
    if title:
        e.title = title
    return e


def ok(d, t="✅ Success"): return E(d, SUCCESS, t)
def err(d, t="❌ Error"): return E(d, ERROR, t)
def wrn(d, t="⚠️ Warning"): return E(d, WARN, t)
def inf(d, t="ℹ️ Information"): return E(d, INFO, t)
def pnl(t, d, c=NEUTRAL): return E(d, c, t)


def with_user(e: discord.Embed, u: discord.abc.User):
    e.set_author(name=str(u), icon_url=u.display_avatar.url)
    e.set_thumbnail(url=u.display_avatar.url)
    return e


def link_ok() -> bool:
    return WEB_URL.startswith("http")


def can_act(guild: discord.Guild, actor: discord.Member, target: discord.Member) -> str | None:
    if target.id == actor.id:
        return "You can't target yourself."
    if target.id == guild.owner_id:
        return "That member is the server owner."
    if actor.id != guild.owner_id and target.top_role >= actor.top_role:
        return "That member's top role is equal to or above yours."
    if guild.me.top_role <= target.top_role:
        return "My role is below theirs — drag my role higher."
    return None


def fmt_dur(secs: int) -> str:
    h, m = secs // 3600, (secs % 3600) // 60
    return f"{h}h {m}m" if h else f"{m}m"


def parse_ids(raw: str, guild: discord.Guild) -> list[discord.Member]:
    out = []
    for token in raw.replace("<@", "").replace(">", "").replace("!", "").replace(",", " ").split():
        if token.isdigit():
            m = guild.get_member(int(token))
            if m and m not in out:
                out.append(m)
    return out


# ─────────────────────────── shared actions ───────────────────────────

async def act_ban(guild, mod, member, reason, days=0):
    try:
        await member.send(embed=wrn(f"You were banned from **{guild.name}**.\n**Reason:** {reason}", "🔨 Banned"))
    except discord.HTTPException:
        pass
    await guild.ban(member, reason=f"{mod} — {reason}", delete_message_days=days)
    e = with_user(err(f"{member.mention} banned.\n**Reason:** {reason}\n**Moderator:** {mod.mention}", "🔨 Member Banned"), member)
    await log_to(guild, "mod_log", e)
    return e


async def act_kick(guild, mod, member, reason):
    try:
        await member.send(embed=wrn(f"You were kicked from **{guild.name}**.\n**Reason:** {reason}", "👢 Kicked"))
    except discord.HTTPException:
        pass
    await member.kick(reason=f"{mod} — {reason}")
    e = with_user(err(f"{member.mention} kicked.\n**Reason:** {reason}\n**Moderator:** {mod.mention}", "👢 Member Kicked"), member)
    await log_to(guild, "mod_log", e)
    return e


async def act_softban(guild, mod, member, reason):
    await guild.ban(member, reason=f"Softban — {mod} — {reason}", delete_message_days=7)
    await guild.unban(member, reason="Softban release")
    e = with_user(err(f"{member.mention} softbanned.\n**Reason:** {reason}\n**Moderator:** {mod.mention}", "🧹 Softbanned"), member)
    await log_to(guild, "mod_log", e)
    return e


async def act_timeout(guild, mod, member, minutes, reason):
    until = discord.utils.utcnow() + timedelta(minutes=minutes)
    await member.timeout(until, reason=f"{mod} — {reason}")
    e = with_user(wrn(f"{member.mention} muted **{minutes}m**.\n**Ends:** <t:{int(until.timestamp())}:R>\n"
                      f"**Reason:** {reason}\n**Moderator:** {mod.mention}", "🔇 Timed Out"), member)
    await log_to(guild, "mod_log", e)
    return e


async def act_warn(guild, mod, member, reason):
    await ex("INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES (?,?,?,?,?)",
             (guild.id, member.id, mod.id, reason, discord.utils.utcnow().isoformat()))
    n = (await one("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?", (guild.id, member.id)))["c"]
    try:
        await member.send(embed=wrn(f"You were warned in **{guild.name}**.\n**Reason:** {reason}\n**Total warnings:** {n}", "⚠️ Warning"))
    except discord.HTTPException:
        pass
    e = with_user(wrn(f"{member.mention} warned (`#{n}`).\n**Reason:** {reason}\n**Moderator:** {mod.mention}", "⚠️ Warned"), member)
    await log_to(guild, "mod_log", e)
    return e


async def act_unban(guild, mod, user, reason):
    await guild.unban(user, reason=f"{mod} — {reason}")
    e = with_user(ok(f"{user.mention} unbanned.\n**Reason:** {reason}\n**Moderator:** {mod.mention}", "🔓 Unbanned"), user)
    await log_to(guild, "mod_log", e)
    return e


# ─────────────────────────── staff core ───────────────────────────

async def award_points(guild: discord.Guild, member: discord.Member, amount: int, reason: str):
    r = await ensure_staff(guild.id, member.id)
    new = max(0, r["points"] + amount)
    if new == r["points"] and amount != 0:
        return
    await ex("UPDATE staff SET points=? WHERE guild_id=? AND user_id=?", (new, guild.id, member.id))
    e = with_user(pnl("⭐ Points", f"{member.mention} **{'+' if amount > 0 else ''}{amount}** — {reason}\n**Total:** `{new}`",
                      SUCCESS if amount > 0 else ERROR), member)
    await log_to(guild, "log_channel", e)
    if amount > 0:
        await promote_check(guild, member)


async def promote_check(guild: discord.Guild, member: discord.Member):
    r = await ensure_staff(guild.id, member.id)
    ranks = await all_("SELECT * FROM staff_ranks WHERE guild_id=? ORDER BY threshold ASC", (guild.id,))
    eligible = [x for x in ranks if x["threshold"] <= r["points"]]
    if not eligible:
        return
    target = eligible[-1]
    role = guild.get_role(target["role_id"])
    if not role or role in member.roles:
        return
    try:
        await member.add_roles(role, reason=f"Automatic promotion — {r['points']} points")
    except discord.HTTPException:
        return
    await log_to(guild, "log_channel", with_user(
        pnl("📈 Promotion", f"{member.mention} promoted to **{target['name']}** at `{r['points']}` points.", SUCCESS), member))


async def staff_signin(guild: discord.Guild, member: discord.Member):
    r = await ensure_staff(guild.id, member.id)
    now = discord.utils.utcnow()
    if r["on_duty"]:
        return False, "on duty", wrn(f"Already on duty since <t:{int(datetime.fromisoformat(r['duty_start']).timestamp())}:t>.")
    if r["last_signin"] == now.date().isoformat():
        return False, "done", wrn("You already completed your shift today. Come back tomorrow. 📅")
    days = r["days_present"] + 1
    await ex("UPDATE staff SET on_duty=1, duty_start=?, last_signin=?, last_message=?, last_active=?, days_present=? "
             "WHERE guild_id=? AND user_id=?",
             (now.isoformat(), now.date().isoformat(), now.isoformat(), now.isoformat(), days, guild.id, member.id))
    e = with_user(pnl("🟢 Staff On Duty", f"{member.mention} signed in.\n**Time:** <t:{int(now.timestamp())}:T>\n"
                      f"**Days present:** `{days}`", SUCCESS), member)
    e.add_field(name="Role", value=member.top_role.mention)
    await log_to(guild, "log_channel", e)
    return True, "ok", e


async def staff_signout(guild: discord.Guild, member: discord.Member, source: str = "word"):
    r = await ensure_staff(guild.id, member.id)
    if not r["on_duty"]:
        return False, "off", wrn("You're not signed in.")
    now = discord.utils.utcnow()
    start = datetime.fromisoformat(r["duty_start"])
    secs = max(1, int((now - start).total_seconds()))
    await ex("UPDATE staff SET on_duty=0, duty_start=NULL, total_seconds=total_seconds+?, active_seconds=0 "
             "WHERE guild_id=? AND user_id=?", (secs, guild.id, member.id))
    await ex("INSERT INTO duty (guild_id, user_id, start_ts, end_ts, seconds) VALUES (?,?,?,?,?)",
             (guild.id, member.id, start.isoformat(), now.isoformat(), secs))
    label = "🔴 Staff Off Duty" if source == "word" else "🔴 Auto Sign-Out"
    note = "" if source == "word" else "\n**Reason:** idle for over an hour"
    e = with_user(pnl(label, f"{member.mention} signed out.\n**Shift:** `{fmt_dur(secs)}`\n"
                      f"**Days present:** `{r['days_present']}`{note}", ERROR), member)
    await log_to(guild, "log_channel", e)
    return True, "ok", e


# ─────────────────────────── UI: verify / welcome / staff ───────────────────────────

class VerifyView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        if link_ok():
            self.add_item(discord.ui.Button(label="Verify", emoji="🔐", style=discord.ButtonStyle.link,
                                            url=f"{WEB_URL}/verify?g={gid}"))


class WelcomeView(discord.ui.View):
    def __init__(self, gid: int, rules: str | None = None):
        super().__init__(timeout=None)
        if link_ok():
            self.add_item(discord.ui.Button(label="Verify", emoji="🔐", style=discord.ButtonStyle.link,
                                            url=f"{WEB_URL}/verify?g={gid}"))
        if rules and rules.startswith("http"):
            self.add_item(discord.ui.Button(label="Rules", emoji="📜", style=discord.ButtonStyle.link, url=rules))

    @discord.ui.button(label="Say hi", emoji="👋", style=discord.ButtonStyle.secondary, custom_id="wv:hi", row=1)
    async def hi(self, it: discord.Interaction, b: discord.ui.Button):
        await it.response.send_message(embed=inf(f"Welcome to **{it.guild.name}**, {it.user.mention}! Glad you're here. 🎉"), ephemeral=True)


class StaffPanelView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.gid = gid

    @discord.ui.button(label="Sign In", emoji="🟢", style=discord.ButtonStyle.success, custom_id="duty:in")
    async def sign_in(self, it: discord.Interaction, b: discord.ui.Button):
        good, _, emb = await staff_signin(it.guild, it.user)
        await it.response.send_message(embed=emb, ephemeral=not good)

    @discord.ui.button(label="Sign Out", emoji="🔴", style=discord.ButtonStyle.danger, custom_id="duty:out")
    async def sign_out(self, it: discord.Interaction, b: discord.ui.Button):
        good, _, emb = await staff_signout(it.guild, it.user, "panel")
        await it.response.send_message(embed=emb, ephemeral=not good)

    @discord.ui.button(label="My Stats", emoji="📊", style=discord.ButtonStyle.secondary, custom_id="duty:stats")
    async def stats(self, it: discord.Interaction, b: discord.ui.Button):
        r = await ensure_staff(self.gid, it.user.id)
        e = with_user(pnl("📊 Your Activity",
                          f"**Points:** `{r['points']}`\n**Days present:** `{r['days_present']}`\n"
                          f"**Total duty time:** `{fmt_dur(r['total_seconds'])}`\n"
                          f"**On duty:** {'🟢 Yes' if r['on_duty'] else '🔴 No'}"), it.user)
        await it.response.send_message(embed=e, ephemeral=True)


# ─────────────────────────── UI: temp voice ───────────────────────────

class VoiceModal(discord.ui.Modal):
    def __init__(self, gid: int, private: bool):
        super().__init__(title="Private Room" if private else "Public Room")
        self.gid = gid
        self.private = private
        self.name = discord.ui.TextInput(label="Channel name", max_length=60,
                                         placeholder="Late night chill")
        self.hours = discord.ui.TextInput(label="Duration in hours (1-24)", max_length=2, default="2")
        self.members = discord.ui.TextInput(
            label="Members allowed (private only)", required=False, style=discord.TextStyle.paragraph,
            placeholder="IDs or mentions, comma separated", max_length=300)
        self.add_item(self.name)
        self.add_item(self.hours)
        self.add_item(self.members)

    async def on_submit(self, it: discord.Interaction):
        await it.response.defer(ephemeral=True)
        s = await settings(self.gid)
        hub = it.guild.get_channel(s["voice_hub"]) if s["voice_hub"] else None

        try:
            hours = max(1, min(24, int(str(self.hours))))
        except ValueError:
            hours = 2

        allowed = parse_ids(str(self.members), it.guild) if self.private else []
        ow = {it.guild.default_role: discord.PermissionOverwrite(view_channel=not self.private,
                                                                connect=not self.private)}
        ow[it.user] = discord.PermissionOverwrite(view_channel=True, connect=True, manage_channels=True)
        for m in allowed:
            ow[m] = discord.PermissionOverwrite(view_channel=True, connect=True)

        try:
            ch = await it.guild.create_voice_channel(
                name=str(self.name), category=hub.category if hub else None, overwrites=ow,
                reason=f"Temp voice by {it.user}")
        except discord.HTTPException as e:
            return await it.followup.send(embed=err(f"Could not create the channel: {e}"), ephemeral=True)

        expires = discord.utils.utcnow() + timedelta(hours=hours)
        await ex("INSERT INTO temp_voice (channel_id, guild_id, owner_id, expires_at, private) VALUES (?,?,?,?,?)",
                 (ch.id, self.gid, it.user.id, expires.isoformat(), int(self.private)))

        try:
            await it.user.move_to(ch)
        except discord.HTTPException:
            pass

        e = with_user(pnl("🔊 Room Created", f"{it.user.mention} opened {ch.mention}\n"
                          f"**Type:** {'🔒 Private' if self.private else '🌐 Public'}\n"
                          f"**Deletes:** <t:{int(expires.timestamp())}:R>", SUCCESS), it.user)
        if allowed:
            e.add_field(name="Allowed", value=" ".join(m.mention for m in allowed[:20]), inline=False)
        if hub:
            try:
                await hub.send(embed=e)
            except discord.HTTPException:
                pass
        await it.followup.send(embed=ok(f"Created {ch.mention} — it vanishes <t:{int(expires.timestamp())}:R>."),
                               ephemeral=True)


class VoiceHubView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.gid = gid
        sel = discord.ui.Select(placeholder="Choose a room type…", custom_id="vc:type",
                                options=[
                                    discord.SelectOption(label="Public", emoji="🌐", value="public",
                                                         description="Anyone can join"),
                                    discord.SelectOption(label="Private", emoji="🔒", value="private",
                                                         description="Only you and chosen members"),
                                ])
        sel.callback = self.pick
        self.add_item(sel)

    async def pick(self, it: discord.Interaction):
        await it.response.send_modal(VoiceModal(self.gid, it.data["values"][0] == "private"))


# ─────────────────────────── UI: tickets ───────────────────────────

class TicketModal(discord.ui.Modal):
    def __init__(self, gid: int, ttype: str):
        super().__init__(title=f"New ticket — {ttype}"[:45])
        self.gid = gid
        self.ttype = ttype
        self.reason = discord.ui.TextInput(label="Reason", style=discord.TextStyle.paragraph,
                                           max_length=900, placeholder="Describe your issue…")
        self.add_item(self.reason)

    async def on_submit(self, it: discord.Interaction):
        await it.response.defer(ephemeral=True)
        s = await settings(self.gid)
        cat = it.guild.get_channel(s["ticket_category"]) if s["ticket_category"] else None
        if cat is None:
            return await it.followup.send(embed=err("Ticket category is missing."), ephemeral=True)

        existing = await one("SELECT * FROM tickets WHERE guild_id=? AND user_id=? AND status='open'",
                             (self.gid, it.user.id))
        if existing:
            ch = it.guild.get_channel(existing["channel_id"])
            if ch:
                return await it.followup.send(embed=wrn(f"You already have a ticket: {ch.mention}"), ephemeral=True)
            await ex("UPDATE tickets SET status='closed' WHERE id=?", (existing["id"],))

        ow = {it.guild.default_role: discord.PermissionOverwrite(view_channel=False),
              it.user: discord.PermissionOverwrite(view_channel=True, send_messages=True, attach_files=True,
                                                   read_message_history=True)}
        staff_role = it.guild.get_role(s["ticket_staff"]) if s["ticket_staff"] else None
        if staff_role:
            ow[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True,
                                                         manage_channels=True, read_message_history=True)

        try:
            ch = await it.guild.create_text_channel(
                name=f"{self.ttype}-{it.user.name}"[:95], category=cat, overwrites=ow,
                topic=f"ticket:{it.user.id}", reason=f"Ticket by {it.user}")
        except discord.HTTPException as e:
            return await it.followup.send(embed=err(f"Could not create channel: {e}"), ephemeral=True)

        await ex("INSERT INTO tickets (guild_id,channel_id,user_id,type,reason,created_at) VALUES (?,?,?,?,?,?)",
                 (self.gid, ch.id, it.user.id, self.ttype, str(self.reason), discord.utils.utcnow().isoformat()))

        e = with_user(pnl("🎫 Ticket Opened", f"{it.user.mention} opened a **{self.ttype}** ticket.\n\n"
                          f"**Reason:**\n{self.reason}", DEFAULT), it.user)
        e.add_field(name="Controls", value="`/ticket close` · `/ticket claim` · `/ticket add`")
        await ch.send(content=it.user.mention, embed=e, view=TicketControlView(self.gid))
        await it.followup.send(embed=ok(f"Ticket created: {ch.mention}"), ephemeral=True)

        if s["ticket_log"] and (lc := it.guild.get_channel(s["ticket_log"])):
            try:
                await lc.send(embed=pnl("🎫 Ticket Opened", f"{it.user.mention} → {ch.mention}\n**Type:** {self.ttype}"))
            except discord.HTTPException:
                pass


class TicketPanelView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.gid = gid
        self._built = False

    async def build(self):
        if self._built:
            return
        rows = await all_("SELECT * FROM ticket_types WHERE guild_id=?", (self.gid,))
        if not rows:
            return
        sel = discord.ui.Select(placeholder="Open a ticket…", custom_id="tk:type", options=[
            discord.SelectOption(label=r["name"][:90], emoji=r["emoji"] or "🎫", value=r["name"],
                                 description=(r["description"] or "")[:90]) for r in rows[:25]])
        sel.callback = self.pick
        self.add_item(sel)
        self._built = True

    async def pick(self, it: discord.Interaction):
        await it.response.send_modal(TicketModal(self.gid, it.data["values"][0]))


class TicketControlView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.gid = gid

    @discord.ui.button(label="Close", emoji="🔒", style=discord.ButtonStyle.danger, custom_id="tk:close")
    async def close(self, it: discord.Interaction, b: discord.ui.Button):
        await it.response.defer()
        t = await one("SELECT * FROM tickets WHERE channel_id=?", (it.channel.id))
        if not t:
            return await it.followup.send(embed=err("This isn't a tracked ticket."))
        await it.followup.send(embed=ok("Closing and saving transcript…"))
        await close_ticket(it.guild, it.channel, t, it.user)

    @discord.ui.button(label="Claim", emoji="🙋", style=discord.ButtonStyle.success, custom_id="tk:claim")
    async def claim(self, it: discord.Interaction, b: discord.ui.Button):
        t = await one("SELECT * FROM tickets WHERE channel_id=?", (it.channel.id))
        if not t:
            return await it.response.send_message(embed=err("Not a tracked ticket."), ephemeral=True)
        await ex("UPDATE tickets SET claimed_by=? WHERE id=?", (it.user.id, t["id"]))
        await it.response.send_message(embed=ok(f"Claimed by {it.user.mention}."))


async def close_ticket(guild, channel, ticket, closer):
    lines = []
    try:
        async for m in channel.history(limit=1000, oldest_first=True):
            lines.append(f"[{m.created_at:%Y-%m-%d %H:%M:%S}] {m.author}: {m.content}")
    except discord.HTTPException:
        pass
    s = await settings(guild.id)
    if s["ticket_log"] and (lc := guild.get_channel(s["ticket_log"])):
        e = with_user(pnl("🎫 Ticket Closed", f"**{channel.name}** closed by {closer.mention}\n"
                          f"**Opener:** <@{ticket['user_id']}>\n**Type:** {ticket['type']}", NEUTRAL), closer)
        try:
            await lc.send(embed=e, file=discord.File(io.BytesIO("\n".join(lines).encode()), filename=f"{channel.name}.txt"))
        except discord.HTTPException:
            pass
    await ex("UPDATE tickets SET status='closed' WHERE id=?", (ticket["id"],))
    try:
        await channel.delete(reason=f"Closed by {closer}")
    except discord.HTTPException:
# ─────────────────────────── bot ───────────────────────────

class Bot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=commands.when_mentioned_or(PREFIX), intents=discord.Intents.all(),
                         help_command=None,
                         allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, replied_user=True))
        self.join_cache: dict[int, deque] = defaultdict(lambda: deque(maxlen=50))
        self.raid_mode: dict[int, float] = {}
        self.spam: dict[tuple, deque] = defaultdict(lambda: deque(maxlen=8))
        self.invites: dict[int, dict[str, int]] = {}

    async def setup_hook(self):
        await db_init()
        for c in (Moderation, AutoMod, AntiRaid, Welcomer, Verification, Logs, Staff,
                  Voice, Tickets, Partners, Invites, Core, Prefix):
            try:
                await self.add_cog(c(self))
            except Exception:
                log.exception("Failed to load %s", c.__name__)
        await self.restore()
        try:
            await self.tree.sync()
        except Exception:
            log.exception("Command sync failed — continuing")
        self.staff_tick.start()
        self.voice_tick.start()
        log.info("Commands synced")

    async def restore(self):
        for g in self.guilds:
            await settings(g.id)
        for row in await all_("SELECT * FROM guilds"):
            g = dict(row)
            if g["verify_enabled"]:
                self.add_view(VerifyView(g["guild_id"]))
            if g["welcome_channel"]:
                self.add_view(WelcomeView(g["guild_id"]))
            if g["staff_channel"] or g["signin_channel"]:
                self.add_view(StaffPanelView(g["guild_id"]))
            if g["voice_hub"]:
                self.add_view(VoiceHubView(g["guild_id"]))
            if g["ticket_panel"]:
                v = TicketPanelView(g["guild_id"])
                await v.build()
                self.add_view(v)
            self.add_view(TicketControlView(g["guild_id"]))

    async def cache_invites(self, guild: discord.Guild):
        try:
            invites = await guild.invites()
        except discord.HTTPException:
            return
        self.invites[guild.id] = {i.code: i.uses or 0 for i in invites}

    async def on_ready(self):
        log.info("Ready as %s — %s guilds", self.user, len(self.guilds))
        await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="the gates 👀"))
        for g in self.guilds:
            await self.cache_invites(g)

    async def close(self):
        self.staff_tick.cancel()
        self.voice_tick.cancel()
        await db_close()
        await super().close()

    async def on_guild_join(self, g: discord.Guild):
        await settings(g.id)
        await self.cache_invites(g)
        if g.system_channel and g.system_channel.permissions_for(g.me).send_messages:
            await g.system_channel.send(embed=pnl("👋 Thanks for adding me!",
                                                  "Run `/setup` for a checklist and `/help` for all commands."))

    async def on_command_error(self, ctx, e):
        if isinstance(e, commands.CommandNotFound):
            return
        if isinstance(e, commands.MissingPermissions):
            return await ctx.send(embed=err("You don't have permission to use that."))
        if isinstance(e, commands.MissingRequiredArgument):
            return await ctx.send(embed=err(f"Missing argument: `{e.param.name}`"))
        if isinstance(e, commands.BadArgument):
            return await ctx.send(embed=err(str(e)))
        if isinstance(e, commands.BotMissingPermissions):
            return await ctx.send(embed=err("I'm missing permissions for that."))
        log.exception("prefix command error", exc_info=e)
        await ctx.send(embed=err(str(e)[:200]))

    @tasks.loop(minutes=1)
    async def staff_tick(self):
        now = discord.utils.utcnow()
        try:
            for r in await all_("SELECT * FROM staff WHERE on_duty=1"):
                guild = self.get_guild(r["guild_id"])
                member = guild.get_member(r["user_id"]) if guild else None
                if not member:
                    continue
                last = datetime.fromisoformat(r["last_message"]) if r["last_message"] else None
                if last and (now - last).total_seconds() <= 600:
                    acc = r["active_seconds"] + 60
                    await ex("UPDATE staff SET active_seconds=? WHERE guild_id=? AND user_id=?",
                             (acc, guild.id, member.id))
                    if acc >= 3600:
                        await ex("UPDATE staff SET active_seconds=active_seconds-3600 WHERE guild_id=? AND user_id=?",
                                 (guild.id, member.id))
                        await award_points(guild, member, 5, "1 hour on duty")
                if last and (now - last).total_seconds() > 3600:
                    await staff_signout(guild, member, "auto")

            for r in await all_("SELECT * FROM staff"):
                guild = self.get_guild(r["guild_id"])
                member = guild.get_member(r["user_id"]) if guild else None
                if not member:
                    continue
                last_active = datetime.fromisoformat(r["last_active"]) if r["last_active"] else None
                if not last_active or (now - last_active).total_seconds() < 86400:
                    continue
                last_pen = datetime.fromisoformat(r["last_penalty"]) if r["last_penalty"] else None
                if last_pen and (now - last_pen).total_seconds() < 86400:
                    continue
                await ex("UPDATE staff SET last_penalty=? WHERE guild_id=? AND user_id=?",
                         (now.isoformat(), guild.id, member.id))
                await award_points(guild, member, -20, "24h inactivity")
        except Exception:
            log.exception("staff tick failed")

    @staff_tick.before_loop
    async def before_tick(self):
        await self.wait_until_ready()

    @tasks.loop(seconds=45)
    async def voice_tick(self):
        now = discord.utils.utcnow()
        try:
            for r in await all_("SELECT * FROM temp_voice"):
                if datetime.fromisoformat(r["expires_at"]) > now:
                    continue
                ch = self.get_channel(r["channel_id"])
                if ch:
                    try:
                        await ch.delete(reason="Temp voice expired")
                    except discord.HTTPException:
                        pass
                await ex("DELETE FROM temp_voice WHERE channel_id=?", (r["channel_id"],))
        except Exception:
            log.exception("voice tick failed")

    @voice_tick.before_loop
    async def before_voice_tick(self):
        await self.wait_until_ready()


bot = Bot()


@bot.tree.error
async def on_app_error(it: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.MissingPermissions):
        msg = "You don't have permission to use that."
    elif isinstance(error, app_commands.BotMissingPermissions):
        msg = "I'm missing permissions for that."
    elif isinstance(error, app_commands.CheckFailure):
        msg = str(error) or "You can't use that here."
    else:
        log.exception("slash command error", exc_info=error)
        msg = str(error)[:200] or "Something went wrong."
    embed = err(msg)
    try:
        if it.response.is_done():
            await it.followup.send(embed=embed, ephemeral=True)
        else:
            await it.response.send_message(embed=embed, ephemeral=True)
    except discord.HTTPException:
        pass


async def log_to(guild, key: str, embed: discord.Embed):
    s = await settings(guild.id)
    cid = s.get(key) or s.get("log_channel")
    ch = guild.get_channel(cid) if cid else None
    if ch and ch.permissions_for(guild.me).send_messages:
        try:
            await ch.send(embed=embed)
        except discord.HTTPException:
            pass


def mod_check():
    async def p(it: discord.Interaction):
        pm = it.user.guild_permissions
        if pm.administrator or pm.ban_members or pm.kick_members or pm.manage_messages:
            return True
        raise app_commands.CheckFailure("You need moderator permissions.")
    return app_commands.check(p)


# ─────────────────────────── moderation ───────────────────────────

class Moderation(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="ban", description="Ban a member")
    @mod_check()
    async def ban(self, it, member: discord.Member, reason: str = "No reason given",
                  delete_days: app_commands.Range[int, 0, 7] = 0):
        if why := can_act(it.guild, it.user, member):
            return await it.response.send_message(embed=err(why), ephemeral=True)
        await it.response.defer()
        await it.followup.send(embed=await act_ban(it.guild, it.user, member, reason, delete_days))

    @app_commands.command(name="unban", description="Unban a user by ID")
    @mod_check()
    async def unban(self, it, user_id: str, reason: str = "No reason given"):
        await it.response.defer()
        try:
            u = await self.bot.fetch_user(int(user_id))
        except (ValueError, discord.NotFound):
            return await it.followup.send(embed=err("No such user."))
        try:
            await it.followup.send(embed=await act_unban(it.guild, it.user, u, reason))
        except discord.NotFound:
            await it.followup.send(embed=err("That user isn't banned."))

    @app_commands.command(name="kick", description="Kick a member")
    @mod_check()
    async def kick(self, it, member: discord.Member, reason: str = "No reason given"):
        if why := can_act(it.guild, it.user, member):
            return await it.response.send_message(embed=err(why), ephemeral=True)
        await it.response.defer()
        await it.followup.send(embed=await act_kick(it.guild, it.user, member, reason))

    @app_commands.command(name="softban", description="Ban then unban to purge messages")
    @mod_check()
    async def softban(self, it, member: discord.Member, reason: str = "No reason given"):
        if why := can_act(it.guild, it.user, member):
            return await it.response.send_message(embed=err(why), ephemeral=True)
        await it.response.defer()
        await it.followup.send(embed=await act_softban(it.guild, it.user, member, reason))

    @app_commands.command(name="timeout", description="Time out a member")
    @mod_check()
    async def timeout(self, it, member: discord.Member, minutes: app_commands.Range[int, 1, 40320],
                      reason: str = "No reason given"):
        if why := can_act(it.guild, it.user, member):
            return await it.response.send_message(embed=err(why), ephemeral=True)
        await it.response.defer()
        await it.followup.send(embed=await act_timeout(it.guild, it.user, member, minutes, reason))

    @app_commands.command(name="untimeout", description="Remove a timeout")
    @mod_check()
    async def untimeout(self, it, member: discord.Member):
        await member.timeout(None, reason=str(it.user))
        await it.response.send_message(embed=ok(f"{member.mention} is no longer muted."))

    @app_commands.command(name="warn", description="Warn a member")
    @mod_check()
    async def warn(self, it, member: discord.Member, reason: str):
        await it.response.defer()
        await it.followup.send(embed=await act_warn(it.guild, it.user, member, reason))

    @app_commands.command(name="warnings", description="List a member's warnings")
    async def warnings(self, it, member: discord.Member):
        await it.response.defer()
        rows = await all_("SELECT * FROM warnings WHERE guild_id=? AND user_id=? ORDER BY id DESC",
                          (it.guild.id, member.id))
        if not rows:
            return await it.followup.send(embed=inf(f"{member.mention} has no warnings."))
        body = "\n".join(f"`#{r['id']}` {r['reason']} — <@{r['mod_id']}>" for r in rows[:15])
        await it.followup.send(embed=with_user(pnl(f"📋 {len(rows)} warning(s)", body, WARN), member))

    @app_commands.command(name="delwarn", description="Delete a warning by ID")
    @mod_check()
    async def delwarn(self, it, warning_id: int):
        cur = await ex("DELETE FROM warnings WHERE id=? AND guild_id=?", (warning_id, it.guild.id))
        if cur.rowcount == 0:
            return await it.response.send_message(embed=err("No such warning."), ephemeral=True)
        await it.response.send_message(embed=ok(f"Warning `#{warning_id}` deleted."))

    @app_commands.command(name="purge", description="Bulk delete messages")
    @mod_check()
    async def purge(self, it, amount: app_commands.Range[int, 1, 100], member: discord.Member | None = None):
        await it.response.defer(ephemeral=True)
        d = await it.channel.purge(limit=amount, check=(lambda m: m.author.id == member.id) if member else None)
        await it.followup.send(embed=ok(f"Deleted **{len(d)}** message(s)."), ephemeral=True)

    @app_commands.command(name="slowmode", description="Set channel slowmode")
    @mod_check()
    async def slowmode(self, it, seconds: app_commands.Range[int, 0, 21600]):
        await it.channel.edit(slowmode_delay=seconds)
        await it.response.send_message(embed=ok(f"Slowmode set to **{seconds}s** in {it.channel.mention}."))

    @app_commands.command(name="lock", description="Lock a channel")
    @mod_check()
    async def lock(self, it, channel: discord.TextChannel | None = None):
        c = channel or it.channel
        ow = c.overwrites_for(it.guild.default_role)
        ow.send_messages = False
        await c.set_permissions(it.guild.default_role, overwrite=ow)
        await it.response.send_message(embed=ok(f"🔒 {c.mention} locked."))

    @app_commands.command(name="unlock", description="Unlock a channel")
    @mod_check()
    async def unlock(self, it, channel: discord.TextChannel | None = None):
        c = channel or it.channel
        ow = c.overwrites_for(it.guild.default_role)
        ow.send_messages = None
        await c.set_permissions(it.guild.default_role, overwrite=ow)
        await it.response.send_message(embed=ok(f"🔓 {c.mention} unlocked."))

    @app_commands.command(name="nick", description="Change a nickname")
    @mod_check()
    async def nick(self, it, member: discord.Member, nickname: str | None = None):
        await member.edit(nick=nickname)
        await it.response.send_message(embed=ok(f"Nickname updated for {member.mention}."))

    @app_commands.command(name="addrole", description="Give a role")
    @mod_check()
    async def addrole(self, it, member: discord.Member, role: discord.Role):
        if role >= it.guild.me.top_role:
            return await it.response.send_message(embed=err("That role is above mine."), ephemeral=True)
        await member.add_roles(role, reason=str(it.user))
        await it.response.send_message(embed=ok(f"Gave {role.mention} to {member.mention}."))

    @app_commands.command(name="removerole", description="Take a role")
    @mod_check()
    async def removerole(self, it, member: discord.Member, role: discord.Role):
        await member.remove_roles(role, reason=str(it.user))
        await it.response.send_message(embed=ok(f"Removed {role.mention} from {member.mention}."))


# ─────────────────────────── automod ───────────────────────────

class AutoMod(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="automod", description="Toggle automod")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def toggle(self, it, enabled: bool):
        await setg(it.guild.id, automod_enabled=int(enabled))
        await it.response.send_message(embed=ok(f"Automod {'enabled' if enabled else 'disabled'}."))

    @app_commands.command(name="blockword", description="Add a blocked word")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def blockword(self, it, word: str):
        await ex("INSERT OR IGNORE INTO blocked_words VALUES (?,?)", (it.guild.id, word.lower()))
        await it.response.send_message(embed=ok(f"Blocked `{word}`."))

    @app_commands.command(name="unblockword", description="Remove a blocked word")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def unblockword(self, it, word: str):
        await ex("DELETE FROM blocked_words WHERE guild_id=? AND word=?", (it.guild.id, word.lower()))
        await it.response.send_message(embed=ok(f"Unblocked `{word}`."))

    @app_commands.command(name="blockedwords", description="List blocked words")
    async def blockedwords(self, it):
        rows = await all_("SELECT word FROM blocked_words WHERE guild_id=?", (it.guild.id,))
        if not rows:
            return await it.response.send_message(embed=inf("No blocked words."))
        await it.response.send_message(embed=pnl("🚫 Blocked Words", "`" + "`, `".join(r["word"] for r in rows) + "`"))

    @app_commands.command(name="blockword_punish", description="Punishment for blocked words")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(action="delete | warn | timeout | kick | ban")
    async def punish(self, it, action: str, minutes: app_commands.Range[int, 1, 40320] = 10):
        if action not in ("delete", "warn", "timeout", "kick", "ban"):
            return await it.response.send_message(embed=err("Use delete, warn, timeout, kick or ban."))
        await setg(it.guild.id, block_punish=action, block_punish_minutes=minutes)
        extra = f" ({minutes}m)" if action == "timeout" else ""
        await it.response.send_message(embed=ok(f"Punishment set to **{action}**{extra}."))

    @app_commands.command(name="automod_ignore", description="Ignore a channel in automod")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def ignore(self, it, channel: discord.TextChannel):
        await ex("INSERT OR IGNORE INTO ignored_channels VALUES (?,?)", (it.guild.id, channel.id))
        await it.response.send_message(embed=ok(f"Automod will ignore {channel.mention}."))

    @app_commands.command(name="automod_unignore", description="Stop ignoring a channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def unignore(self, it, channel: discord.TextChannel):
        await ex("DELETE FROM ignored_channels WHERE guild_id=? AND channel_id=?", (it.guild.id, channel.id))
        await it.response.send_message(embed=ok(f"Automod will moderate {channel.mention} again."))

    @commands.Cog.listener()
    async def on_message(self, m: discord.Message):
        if not m.guild or m.author.bot or not m.content:
            return
        s = await settings(m.guild.id)
        if not s["automod_enabled"] or m.author.guild_permissions.manage_messages:
            return
        if await one("SELECT 1 FROM ignored_channels WHERE guild_id=? AND channel_id=?", (m.guild.id, m.channel.id)):
            return

        low = m.content.lower()
        words = await all_("SELECT word FROM blocked_words WHERE guild_id=?", (m.guild.id,))
        hits = [r["word"] for r in words if r["word"] in low]

        key = (m.guild.id, m.author.id)
        now = discord.utils.utcnow()
        bucket = self.bot.spam[key]
        bucket.append((now, low))
        recent = [t for t, _ in bucket if (now - t).total_seconds() < 6]
        duplicate = len([t for t, x in bucket if x == low and (now - t).total_seconds() < 20]) >= 3
        spammy = len(recent) >= 5
        mention_spam = len(m.mentions) >= 5

        if not (hits or spammy or duplicate or mention_spam):
            return

        try:
            await m.delete()
        except discord.HTTPException:
            pass

        reason = ("Blocked word: " + ", ".join(hits) if hits else
                  "Message spam" if spammy else
                  "Duplicate spam" if duplicate else "Mention spam")

        await ex("INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES (?,?,?,?,?)",
                 (m.guild.id, m.author.id, self.bot.user.id, f"[AUTOMOD] {reason}", now.isoformat()))
        n = (await one("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?",
                       (m.guild.id, m.author.id)))["c"]

        action = s["block_punish"] or "delete"
        note = f"**{n}/{s['automod_escalation']}**"
        try:
            if hits and action == "timeout":
                await m.author.timeout(now + timedelta(minutes=s["block_punish_minutes"]), reason=reason)
                note += f" — muted {s['block_punish_minutes']}m"
            elif hits and action == "kick":
                await m.author.kick(reason=reason)
            elif hits and action == "ban":
                await m.author.ban(reason=reason, delete_message_days=1)
            elif n >= s["automod_escalation"]:
                await m.author.timeout(now + timedelta(minutes=10), reason="Automod escalation")
                note += " — muted 10m"
        except discord.HTTPException:
            pass

        e = with_user(wrn(f"{m.author.mention} — {reason}\nPunishment: `{action}` · Warnings: {note}", "🤖 Automod"), m.author)
        try:
            await m.channel.send(embed=e, delete_after=12)
        except discord.HTTPException:
            pass
        await log_to(m.guild, "mod_log", e)


# ─────────────────────────── antiraid ───────────────────────────

class AntiRaid(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="antiraid", description="Configure raid protection")
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(action="alert | kick | lockdown")
    async def config(self, it, enabled: bool, threshold: app_commands.Range[int, 2, 50] = 8,
                     window: app_commands.Range[int, 3, 120] = 12, action: str = "alert",
                     min_account_age: app_commands.Range[int, 0, 90] = 0):
        if action not in ("alert", "kick", "lockdown"):
            return await it.response.send_message(embed=err("Use alert, kick or lockdown."), ephemeral=True)
        await setg(it.guild.id, antiraid_enabled=int(enabled), antiraid_threshold=threshold,
                   antiraid_window=window, antiraid_action=action, min_account_age=min_account_age)
        await it.response.send_message(embed=pnl("🛡️ Anti-Raid Configured",
                                                f"**Enabled:** {enabled}\n**Threshold:** {threshold} joins / {window}s\n"
                                                f"**Action:** `{action}`\n**Min account age:** {min_account_age}d"))

    @app_commands.command(name="raidmode", description="Toggle lockdown manually")
    @app_commands.checks.has_permissions(administrator=True)
    async def raidmode(self, it, enabled: bool):
        if enabled:
            self.bot.raid_mode[it.guild.id] = discord.utils.utcnow().timestamp()
            await it.guild.edit(verification_level=discord.VerificationLevel.high)
        else:
            self.bot.raid_mode.pop(it.guild.id, None)
        await it.response.send_message(embed=ok(f"Raid mode {'**ENABLED** 🔴' if enabled else 'disabled 🟢'}."))

    @commands.Cog.listener()
    async def on_member_join(self, m: discord.Member):
        s = await settings(m.guild.id)
        if not s["antiraid_enabled"]:
            return
        age = (discord.utils.utcnow() - m.created_at).days
        if s["min_account_age"] and age < s["min_account_age"]:
            try:
                await m.send(embed=wrn(f"Your account is too new ({age}d) for **{m.guild.name}**.", "🚪 Join Rejected"))
            except discord.HTTPException:
                pass
            try:
                await m.kick(reason=f"Account age {age}d < {s['min_account_age']}d")
            except discord.HTTPException:
                pass
            return

        cache = self.bot.join_cache[m.guild.id]
        now = discord.utils.utcnow().timestamp()
        cache.append(now)
        recent = [t for t in cache if now - t <= s["antiraid_window"]]
        if len(recent) < s["antiraid_threshold"] or self.bot.raid_mode.get(m.guild.id, 0) > now - 30:
            return

        self.bot.raid_mode[m.guild.id] = now
        action = s["antiraid_action"]
        try:
            if action == "kick":
                await m.kick(reason="Raid protection")
            elif action == "lockdown":
                await m.guild.edit(verification_level=discord.VerificationLevel.highest)
        except discord.HTTPException:
            pass

        e = pnl("🚨 Raid Detected", f"**{len(recent)}** joins in **{s['antiraid_window']}s**\n"
                                   f"**Action:** `{action}`\n**Trigger:** {m.mention} (`{m.id}`, {age}d old)", ERROR)
        await log_to(m.guild, "join_log", e)
        if m.guild.system_channel:
            try:
                await m.guild.system_channel.send(embed=e)
            except discord.HTTPException:
                passpass
# ─────────────────────────── welcomer ───────────────────────────

class Welcomer(commands.Cog):
    def __init__(self, bot): self.bot = bot

    def render(self, text: str, m) -> str:
        g = getattr(m, "guild", None)
        return (text.replace("{user}", getattr(m, "mention", str(m)))
                .replace("{name}", m.name)
                .replace("{server}", g.name if g else "the server")
                .replace("{count}", str(g.member_count if g else 0)))

    @app_commands.command(name="welcome", description="Configure the welcomer")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def welcome(self, it, channel: discord.TextChannel,
                      title: str = "🎉 Welcome to {server}!",
                      message: str = "Hey {user}, welcome aboard!\nYou're member **#{count}**. Read the rules and verify to unlock the server.",
                      color: str = "5865F2", image: str | None = None, rules_url: str | None = None):
        try:
            col = int(color.replace("#", ""), 16)
        except ValueError:
            return await it.response.send_message(embed=err("Colour must be hex like `5865F2`."), ephemeral=True)
        if image and not image.startswith("http"):
            return await it.response.send_message(embed=err("Image must be a URL."), ephemeral=True)
        await setg(it.guild.id, welcome_channel=channel.id, welcome_title=title,
                   welcome_message=message, welcome_color=col, welcome_image=image)
        self.bot.add_view(WelcomeView(it.guild.id, rules_url))
        await it.response.send_message(embed=ok(f"Welcomer set to {channel.mention}. Preview with `/welcome_preview`."))

    @app_commands.command(name="goodbye", description="Configure the goodbye message")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def goodbye(self, it, channel: discord.TextChannel,
                      message: str = "👋 **{name}** left. We're down to **{count}** members."):
        await setg(it.guild.id, goodbye_channel=channel.id, goodbye_message=message)
        await it.response.send_message(embed=ok(f"Goodbye set to {channel.mention}."))

    @app_commands.command(name="welcome_preview", description="Preview the welcome embed")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def preview(self, it):
        await it.response.defer(ephemeral=True)
        s = await settings(it.guild.id)
        try:
            await it.followup.send(embed=self.build(s, it.user), view=WelcomeView(it.guild.id), ephemeral=True)
        except discord.HTTPException as e:
            await it.followup.send(embed=err(f"Could not build preview: {e}"), ephemeral=True)

    def build(self, s: dict, m) -> discord.Embed:
        color = s["welcome_color"] or DEFAULT
        e = E(self.render(s["welcome_message"] or "Welcome {user}!", color),
              color, self.render(s["welcome_title"] or "Welcome!", m))
        if isinstance(m, discord.Member):
            e.set_author(name=str(m), icon_url=m.display_avatar.url)
            e.set_thumbnail(url=m.display_avatar.url)
            e.set_footer(text=f"Member #{m.guild.member_count} · {m.guild.name}")
        else:
            e.set_footer(text="Preview")
        if s["welcome_image"]:
            e.set_image(url=s["welcome_image"])
        return e

    @commands.Cog.listener()
    async def on_member_join(self, m: discord.Member):
        s = await settings(m.guild.id)
        if s["welcome_channel"]:
            ch = m.guild.get_channel(s["welcome_channel"])
            if ch and ch.permissions_for(m.guild.me).send_messages:
                try:
                    await ch.send(content=m.mention, embed=self.build(s, m), view=WelcomeView(m.guild.id))
                except discord.HTTPException as e:
                    log.warning("welcome failed in %s: %s", m.guild.id, e)
        if s["join_log"]:
            ch = m.guild.get_channel(s["join_log"])
            if ch:
                try:
                    await ch.send(embed=with_user(
                        inf(f"{m.mention} joined.\n**Created:** <t:{int(m.created_at.timestamp())}:R>\n**ID:** `{m.id}`",
                            "📥 Member Joined"), m))
                except discord.HTTPException:
                    pass

    @commands.Cog.listener()
    async def on_member_remove(self, m: discord.Member):
        s = await settings(m.guild.id)
        if s["goodbye_channel"]:
            ch = m.guild.get_channel(s["goodbye_channel"])
            if ch:
                try:
                    await ch.send(embed=with_user(
                        pnl("👋 Member Left", self.render(s["goodbye_message"] or "**{name}** left.", m), ERROR), m))
                except discord.HTTPException:
                    pass


# ─────────────────────────── verification ───────────────────────────

class Verification(commands.Cog):
    def __init__(self, bot): self.bot = bot

    group = app_commands.Group(name="verify", description="Verification management")

    @group.command(name="enable", description="Enable verification and post the panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def enable(self, it, channel: discord.TextChannel, role: discord.Role,
                     unverified_role: discord.Role | None = None, log_channel: discord.TextChannel | None = None,
                     title: str = "🔐 Verification Required",
                     description: str = "Click the button below to verify through our website.\nYour IP is logged to keep alts and raiders out."):
        if not link_ok():
            return await it.response.send_message(embed=err("`WEB_URL` is not configured on the host."), ephemeral=True)
        if role >= it.guild.me.top_role:
            return await it.response.send_message(embed=err("That role is above mine — drag my role higher."), ephemeral=True)
        await setg(it.guild.id, verify_enabled=1, verify_role=role.id,
                   unverified_role=unverified_role.id if unverified_role else None,
                   verify_log=log_channel.id if log_channel else None)
        self.bot.add_view(VerifyView(it.guild.id))
        e = pnl(title, description, DEFAULT)
        e.add_field(name="Steps", value="1️⃣ Click **Verify**\n2️⃣ Authorize on the website\n3️⃣ You're in!")
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        try:
            await channel.send(embed=e, view=VerifyView(it.guild.id))
        except discord.HTTPException as ex_:
            return await it.response.send_message(embed=err(f"Could not post panel: {ex_}"), ephemeral=True)
        await it.response.send_message(
            embed=ok(f"Verification **enabled**. Panel posted in {channel.mention}. Role: {role.mention}"), ephemeral=True)

    @group.command(name="disable", description="Disable verification")
    @app_commands.checks.has_permissions(administrator=True)
    async def disable(self, it):
        await setg(it.guild.id, verify_enabled=0)
        await it.response.send_message(embed=ok("Verification disabled."))

    @group.command(name="status", description="Show verification configuration")
    async def status(self, it):
        s = await settings(it.guild.id)
        role = it.guild.get_role(s["verify_role"]) if s["verify_role"] else None
        ch = it.guild.get_channel(s["verify_log"]) if s["verify_log"] else None
        n = (await one("SELECT COUNT(*) c FROM verifications WHERE guild_id=?", (it.guild.id,)))["c"]
        await it.response.send_message(embed=pnl("🔐 Verification Status", "\n".join([
            f"**Enabled:** {'🟢 yes' if s['verify_enabled'] else '🔴 no'}",
            f"**Role:** {role.mention if role else '—'}",
            f"**Log channel:** {ch.mention if ch else '—'}",
            f"**Verified members:** `{n}`",
            f"**Web URL:** `{WEB_URL or 'not set'}`",
        ])), ephemeral=True)

    @group.command(name="alts", description="Show detected alts for a member")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def alts(self, it, member: discord.Member):
        rows = await all_("SELECT * FROM alts WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        v = await one("SELECT * FROM verifications WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not v:
            return await it.response.send_message(embed=inf(f"{member.mention} hasn't verified."), ephemeral=True)
        body = "\n".join(f"• <@{r['matched_user_id']}> — {r['reason']}" for r in rows) or "None"
        await it.response.send_message(embed=with_user(pnl("👥 Alt Check",
            f"**IP:** `{v['ip']}`\n**Verified:** <t:{int(datetime.fromisoformat(v['verified_at']).timestamp())}:R>\n\n"
            f"**Matches:**\n{body}"), member))

    @group.command(name="whois", description="Show a member's verification record")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def whois(self, it, member: discord.Member):
        v = await one("SELECT * FROM verifications WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not v:
            return await it.response.send_message(embed=inf("No record."), ephemeral=True)
        await it.response.send_message(embed=with_user(pnl("🕵️ Verification Record",
            f"**IP:** `{v['ip']}`\n**User agent:** `{v['user_agent'][:80]}`\n"
            f"**Verified:** <t:{int(datetime.fromisoformat(v['verified_at']).timestamp())}:F>"), member))

    @commands.Cog.listener()
    async def on_member_join(self, m: discord.Member):
        s = await settings(m.guild.id)
        if s["verify_enabled"] and s["unverified_role"]:
            role = m.guild.get_role(s["unverified_role"])
            if role:
                try:
                    await m.add_roles(role, reason="Unverified")
                except discord.HTTPException:
                    pass


# ─────────────────────────── logs ───────────────────────────

class Logs(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="logs", description="Set a log channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(kind="all | mod | join | verify")
    async def logs(self, it, channel: discord.TextChannel, kind: str = "all"):
        m = {"all": "log_channel", "mod": "mod_log", "join": "join_log", "verify": "verify_log"}
        if kind not in m:
            return await it.response.send_message(embed=err("Use all, mod, join or verify."), ephemeral=True)
        await setg(it.guild.id, **{m[kind]: channel.id})
        await it.response.send_message(embed=ok(f"`{kind}` logs → {channel.mention}."))

    @commands.Cog.listener()
    async def on_message_delete(self, m: discord.Message):
        if not m.guild or m.author.bot:
            return
        await log_to(m.guild, "log_channel", with_user(
            pnl("🗑️ Message Deleted", f"**Channel:** {m.channel.mention}\n```{m.content[:900] or '—'}```", WARN), m.author))

    @commands.Cog.listener()
    async def on_message_edit(self, a: discord.Message, b: discord.Message):
        if not a.guild or a.author.bot or a.content == b.content:
            return
        await log_to(a.guild, "log_channel", with_user(pnl("✏️ Message Edited",
            f"**Channel:** {a.channel.mention}\n**Before:**\n```{a.content[:400]}```\n**After:**\n```{b.content[:400]}```",
            INFO), a.author))

    @commands.Cog.listener()
    async def on_member_update(self, a: discord.Member, b: discord.Member):
        if a.roles != b.roles:
            added = [r.mention for r in b.roles if r not in a.roles]
            removed = [r.mention for r in a.roles if r not in b.roles]
            if added or removed:
                await log_to(b.guild, "log_channel", with_user(pnl("🎭 Roles Updated",
                    f"**Added:** {', '.join(added) or '—'}\n**Removed:** {', '.join(removed) or '—'}", INFO), b))
        if a.nick != b.nick:
            await log_to(b.guild, "log_channel", with_user(
                pnl("📝 Nickname Changed", f"`{a.nick}` → `{b.nick}`", NEUTRAL), b))

    @commands.Cog.listener()
    async def on_voice_state_update(self, m, before, after):
        if before.channel == after.channel:
            return
        if before.channel is None:
            await log_to(m.guild, "log_channel", with_user(pnl("🔊 Joined Voice", after.channel.mention, SUCCESS), m))
        elif after.channel is None:
            await log_to(m.guild, "log_channel", with_user(pnl("🔈 Left Voice", before.channel.mention, ERROR), m))


# ─────────────────────────── staff ───────────────────────────

class Staff(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="staff_setup", description="Set the sign-in channel and trigger word")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it, channel: discord.TextChannel, signin_word: str, signout_word: str = "signout"):
        if " " in signin_word:
            return await it.response.send_message(embed=err("The word can't contain spaces."), ephemeral=True)
        await setg(it.guild.id, signin_channel=channel.id,
                   signin_word=signin_word.lower(), signout_word=signout_word.lower())
        self.bot.add_view(StaffPanelView(it.guild.id))
        e = pnl("🛡️ Staff Duty Channel",
                f"Type **{signin_word}** in {channel.mention} to start your shift.\n"
                f"Type **{signout_word}** to end it.\n\n"
                "• 1 hour on duty = **+5 points**\n• Every 20 messages a day = **+5 points**\n"
                "• 1 hour idle while on duty = automatic sign-out\n• 24 hours inactive = **−20 points**\n"
                "• Promotions are automatic by points.")
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        await channel.send(embed=e)
        await it.response.send_message(embed=ok(f"Sign-in channel set to {channel.mention} with word `{signin_word}`."),
                                      ephemeral=True)

    @app_commands.command(name="staff", description="Post the staff duty panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def panel(self, it, channel: discord.TextChannel):
        await setg(it.guild.id, staff_channel=channel.id)
        self.bot.add_view(StaffPanelView(it.guild.id))
        e = pnl("🛡️ Staff Duty", "Sign in when you start your shift and sign out when you finish.\n"
                                 "One sign-in per day counts toward **days present**; shift length builds **points** and **rank**.\n\n"
                                 "🟢 **Sign In** · 🔴 **Sign Out** · 📊 **My Stats**")
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        await channel.send(embed=e, view=StaffPanelView(it.guild.id))
        await it.response.send_message(embed=ok(f"Panel posted in {channel.mention}."), ephemeral=True)

    @app_commands.command(name="rank_add", description="Add a promotion rank")
    @app_commands.checks.has_permissions(administrator=True)
    async def rank_add(self, it, name: str, points: app_commands.Range[int, 1, 100000], role: discord.Role):
        if await one("SELECT 1 FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, name)):
            return await it.response.send_message(embed=err("A rank with that name exists."), ephemeral=True)
        n = (await one("SELECT COUNT(*) c FROM staff_ranks WHERE guild_id=?", (it.guild.id,)))["c"]
        await ex("INSERT INTO staff_ranks (guild_id,name,threshold,role_id,position) VALUES (?,?,?,?,?)",
                 (it.guild.id, name, points, role.id, n))
        await it.response.send_message(embed=ok(f"Rank **{name}** at `{points}` points → {role.mention}."))

    @app_commands.command(name="ranks", description="List promotion ranks")
    async def ranks(self, it):
        rows = await all_("SELECT * FROM staff_ranks WHERE guild_id=? ORDER BY threshold", (it.guild.id,))
        if not rows:
            return await it.response.send_message(embed=inf("No ranks configured."), ephemeral=True)
        await it.response.send_message(embed=pnl("📈 Promotion Ladder",
            "\n".join(f"`{r['threshold']}` → **{r['name']}** (<@&{r['role_id']}>)" for r in rows)), ephemeral=True)

    @app_commands.command(name="rank_remove", description="Remove a rank")
    @app_commands.checks.has_permissions(administrator=True)
    async def rank_remove(self, it, name: str):
        cur = await ex("DELETE FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, name))
        if cur.rowcount == 0:
            return await it.response.send_message(embed=err("No such rank."), ephemeral=True)
        await it.response.send_message(embed=ok(f"Removed rank `{name}`."))

    @app_commands.command(name="promote", description="Promote a staff member manually")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def promote(self, it, member: discord.Member, rank: str):
        r = await one("SELECT * FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, rank))
        if not r:
            return await it.response.send_message(embed=err("Unknown rank."), ephemeral=True)
        role = it.guild.get_role(r["role_id"])
        if not role:
            return await it.response.send_message(embed=err("Rank role no longer exists."), ephemeral=True)
        await member.add_roles(role, reason=f"Manual promotion by {it.user}")
        e = with_user(pnl("📈 Promotion", f"{member.mention} promoted to **{rank}** by {it.user.mention}.", SUCCESS), member)
        await it.response.send_message(embed=e)
        await log_to(it.guild, "log_channel", e)

    @app_commands.command(name="demote", description="Remove a rank role")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def demote(self, it, member: discord.Member, rank: str):
        r = await one("SELECT * FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, rank))
        if not r:
            return await it.response.send_message(embed=err("Unknown rank."), ephemeral=True)
        role = it.guild.get_role(r["role_id"])
        if role:
            await member.remove_roles(role, reason=f"Demoted by {it.user}")
        await it.response.send_message(embed=ok(f"{member.mention} demoted from **{rank}**."))

    @app_commands.command(name="points", description="Adjust a staff member's points")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def points(self, it, member: discord.Member, amount: int, reason: str = "Manual adjustment"):
        await award_points(it.guild, member, amount, f"{reason} (by {it.user})")
        r = await ensure_staff(it.guild.id, member.id)
        await it.response.send_message(embed=ok(f"{member.mention} now has `{r['points']}` points."))

    @app_commands.command(name="staffstats", description="View a staff member's record")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def staffstats(self, it, member: discord.Member):
        r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not r:
            return await it.response.send_message(embed=inf("No staff record."), ephemeral=True)
        await it.response.send_message(embed=with_user(pnl("📊 Staff Record",
            f"**Points:** `{r['points']}`\n**Days present:** `{r['days_present']}`\n"
            f"**Total time:** `{fmt_dur(r['total_seconds'])}`\n**On duty:** {'🟢' if r['on_duty'] else '🔴'}\n"
            f"**Last sign-in:** {r['last_signin'] or '—'}\n**Last activity:** {r['last_active'] or '—'}"), member))

    @app_commands.command(name="stafftop", description="Staff activity leaderboard")
    async def stafftop(self, it):
        rows = await all_("SELECT * FROM staff WHERE guild_id=? ORDER BY points DESC LIMIT 10", (it.guild.id,))
        if not rows:
            return await it.response.send_message(embed=inf("No data yet."), ephemeral=True)
        medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
        await it.response.send_message(embed=pnl("🏆 Staff Leaderboard",
            "\n".join(f"{medals[i]} <@{r['user_id']}> — `{r['points']}` pts · `{r['days_present']}` days"
                      for i, r in enumerate(rows))))

    @commands.Cog.listener()
    async def on_message(self, m: discord.Message):
        if not m.guild or m.author.bot or not m.content:
            return
        s = await settings(m.guild.id)

        r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (m.guild.id, m.author.id))
        if r:
            now = discord.utils.utcnow()
            today = now.date().isoformat()
            count = r["msg_count"] + 1 if r["msg_date"] == today else 1
            await ex("UPDATE staff SET msg_count=?, msg_date=?, last_message=?, last_active=? "
                     "WHERE guild_id=? AND user_id=?",
                     (count, today, now.isoformat(), now.isoformat(), m.guild.id, m.author.id))
            if count % 20 == 0:
                await award_points(m.guild, m.author, 5, "20 messages today")

        if not s["signin_channel"] or m.channel.id != s["signin_channel"]:
            return
        word = m.content.strip().lower()
        if s["signin_word"] and word == s["signin_word"]:
            _, _, emb = await staff_signin(m.guild, m.author)
            try:
                await m.channel.send(embed=emb, delete_after=20)
            except discord.HTTPException:
                pass
        elif s["signout_word"] and word == s["signout_word"]:
            _, _, emb = await staff_signout(m.guild, m.author, "word")
            try:
                await m.channel.send(embed=emb, delete_after=20)
            except discord.HTTPException:
# ─────────────────────────── temp voice ───────────────────────────

class Voice(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="voice-setup", description="Set up the temporary voice channel hub")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it, channel: discord.TextChannel):
        await setg(it.guild.id, voice_hub=channel.id)
        self.bot.add_view(VoiceHubView(it.guild.id))
        e = pnl("🔊 Create Your Own Voice Room", "\n".join([
            "**1.** Pick a room type below.",
            "**2.** Fill in a name and how many hours it should last.",
            "**3.** Private rooms need the members you want to allow.",
            "",
            "🌐 **Public** — anyone can drop in.",
            "🔒 **Private** — only you and the people you list.",
            "",
            "Rooms delete themselves when the timer runs out, and close early once empty.",
        ]), DEFAULT)
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        await channel.send(embed=e, view=VoiceHubView(it.guild.id))
        await it.response.send_message(embed=ok(f"Voice hub posted in {channel.mention}."), ephemeral=True)

    @app_commands.command(name="voice-clean", description="Delete all tracked temp rooms")
    @app_commands.checks.has_permissions(administrator=True)
    async def clean(self, it):
        await it.response.defer(ephemeral=True)
        rows = await all_("SELECT * FROM temp_voice WHERE guild_id=?", (it.guild.id,))
        n = 0
        for r in rows:
            ch = it.guild.get_channel(r["channel_id"])
            if ch:
                try:
                    await ch.delete(reason="Voice cleanup")
                    n += 1
                except discord.HTTPException:
                    pass
        await ex("DELETE FROM temp_voice WHERE guild_id=?", (it.guild.id,))
        await it.followup.send(embed=ok(f"Deleted **{n}** temp room(s)."), ephemeral=True)

    @commands.Cog.listener()
    async def on_voice_state_update(self, m, before, after):
        if before.channel is None or not before.channel.members:
            if before.channel is None:
                return
            row = await one("SELECT * FROM temp_voice WHERE channel_id=?", (before.channel.id,))
            if row:
                try:
                    await before.channel.delete(reason="Temp room empty")
                except discord.HTTPException:
                    pass
                await ex("DELETE FROM temp_voice WHERE channel_id=?", (before.channel.id,))


# ─────────────────────────── tickets ───────────────────────────

class Tickets(commands.Cog):
    def __init__(self, bot): self.bot = bot

    group = app_commands.Group(name="ticket", description="Ticket system")

    @group.command(name="setup", description="Set up the ticket system")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it, category: discord.CategoryChannel, panel_channel: discord.TextChannel,
                    log_channel: discord.TextChannel | None = None, staff_role: discord.Role | None = None):
        await setg(it.guild.id, ticket_category=category.id, ticket_panel=panel_channel.id,
                   ticket_log=log_channel.id if log_channel else None,
                   ticket_staff=staff_role.id if staff_role else None)
        if not await one("SELECT 1 FROM ticket_types WHERE guild_id=?", (it.guild.id,)):
            for n, e_, d in (("General", "💬", "Anything that doesn't fit elsewhere"),
                             ("Report", "🚨", "Report a member or a bug")):
                await ex("INSERT OR IGNORE INTO ticket_types VALUES (?,?,?,?)", (it.guild.id, n, e_, d))
        await it.response.send_message(
            embed=ok(f"Configured. Now run `/ticket panel` in {panel_channel.mention}."), ephemeral=True)

    @group.command(name="add_type", description="Add a ticket type")
    @app_commands.checks.has_permissions(administrator=True)
    async def add_type(self, it, name: str, emoji: str = "🎫", description: str = ""):
        n = (await one("SELECT COUNT(*) c FROM ticket_types WHERE guild_id=?", (it.guild.id,)))["c"]
        if n >= 25:
            return await it.response.send_message(embed=err("Maximum 25 types."), ephemeral=True)
        await ex("INSERT OR REPLACE INTO ticket_types VALUES (?,?,?,?)", (it.guild.id, name, emoji, description))
        await it.response.send_message(embed=ok(f"Added type **{name}**."))

    @group.command(name="remove_type", description="Remove a ticket type")
    @app_commands.checks.has_permissions(administrator=True)
    async def remove_type(self, it, name: str):
        cur = await ex("DELETE FROM ticket_types WHERE guild_id=? AND name=?", (it.guild.id, name))
        if cur.rowcount == 0:
            return await it.response.send_message(embed=err("No such type."), ephemeral=True)
        await it.response.send_message(embed=ok(f"Removed type **{name}**."))

    @group.command(name="panel", description="Post the ticket panel")
    @app_commands.checks.has_permissions(administrator=True)
    async def panel(self, it, channel: discord.TextChannel | None = None):
        s = await settings(it.guild.id)
        target = channel or (it.guild.get_channel(s["ticket_panel"]) if s["ticket_panel"] else None)
        if target is None:
            return await it.response.send_message(embed=err("No channel — pass one or run `/ticket setup`."), ephemeral=True)
        view = TicketPanelView(it.guild.id)
        await view.build()
        if not view.children:
            return await it.response.send_message(embed=err("No ticket types configured."), ephemeral=True)
        e = pnl("🎫 Support Tickets", "Pick a category below and describe your issue.\n"
                                      "A private channel opens for you and the staff team.")
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        await target.send(embed=e, view=view)
        self.bot.add_view(view)
        await it.response.send_message(embed=ok(f"Panel posted in {target.mention}."), ephemeral=True)

    @group.command(name="close", description="Close the current ticket")
    async def close(self, it):
        t = await one("SELECT * FROM tickets WHERE channel_id=?", (it.channel.id,))
        if not t:
            return await it.response.send_message(embed=err("This isn't a ticket channel."), ephemeral=True)
        if it.user.id != t["user_id"] and not it.user.guild_permissions.manage_channels:
            return await it.response.send_message(embed=err("Only the opener or staff can close this."), ephemeral=True)
        await it.response.defer()
        await it.followup.send(embed=ok("Closing and saving transcript…"))
        await close_ticket(it.guild, it.channel, t, it.user)

    @group.command(name="claim", description="Claim the current ticket")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def claim(self, it):
        t = await one("SELECT * FROM tickets WHERE channel_id=?", (it.channel.id,))
        if not t:
            return await it.response.send_message(embed=err("Not a ticket channel."), ephemeral=True)
        await ex("UPDATE tickets SET claimed_by=? WHERE id=?", (it.user.id, t["id"]))
        await it.response.send_message(embed=ok(f"Claimed by {it.user.mention}."))

    @group.command(name="add", description="Add a member to this ticket")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def add(self, it, member: discord.Member):
        await it.channel.set_permissions(member, view_channel=True, send_messages=True, read_message_history=True)
        await it.response.send_message(embed=ok(f"Added {member.mention}."))

    @group.command(name="remove", description="Remove a member from this ticket")
    @app_commands.checks.has_permissions(manage_channels=True)
    async def remove(self, it, member: discord.Member):
        await it.channel.set_permissions(member, overwrite=None)
        await it.response.send_message(embed=ok(f"Removed {member.mention}."))

    @group.command(name="stats", description="Ticket statistics")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stats(self, it):
        open_ = (await one("SELECT COUNT(*) c FROM tickets WHERE guild_id=? AND status='open'", (it.guild.id,)))["c"]
        closed = (await one("SELECT COUNT(*) c FROM tickets WHERE guild_id=? AND status='closed'", (it.guild.id,)))["c"]
        await it.response.send_message(embed=pnl("🎫 Ticket Stats", f"**Open:** `{open_}`\n**Closed:** `{closed}`"),
                                      ephemeral=True)


# ─────────────────────────── partners ───────────────────────────

class Partners(commands.Cog):
    def __init__(self, bot): self.bot = bot

    group = app_commands.Group(name="partner", description="Partnership management")

    @group.command(name="setup", description="Configure partnership settings")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it, channel: discord.TextChannel | None = None, role: discord.Role | None = None):
        await setg(it.guild.id,
                   partner_channel=channel.id if channel else None,
                   partner_role=role.id if role else None)
        await it.response.send_message(
            embed=ok(f"Partnership channel: {channel.mention if channel else '—'}\n"
                     f"Partner role: {role.mention if role else '—'}"), ephemeral=True)

    @group.command(name="add", description="Register a partner")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def add(self, it, member: discord.Member, note: str = "No note"):
        await ex("INSERT OR REPLACE INTO partners (guild_id,user_id,note,added_by,created_at) VALUES (?,?,?,?,?)",
                 (it.guild.id, member.id, note, it.user.id, discord.utils.utcnow().isoformat()))
        s = await settings(it.guild.id)
        if s["partner_role"] and (role := it.guild.get_role(s["partner_role"])):
            try:
                await member.add_roles(role, reason="Partnership")
            except discord.HTTPException:
                pass
        e = with_user(pnl("🤝 Partnership Registered", f"{member.mention} is now a partner.\n"
                          f"**Note:** {note}\n**Added by:** {it.user.mention}", SUCCESS), member)
        await it.response.send_message(embed=e)
        if s["partner_channel"] and (ch := it.guild.get_channel(s["partner_channel"])):
            try:
                await ch.send(embed=e)
            except discord.HTTPException:
                pass
        await log_to(it.guild, "log_channel", e)

    @group.command(name="remove", description="Remove a partner")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def remove(self, it, member: discord.Member):
        cur = await ex("DELETE FROM partners WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if cur.rowcount == 0:
            return await it.response.send_message(embed=err("Not a registered partner."), ephemeral=True)
        s = await settings(it.guild.id)
        if s["partner_role"] and (role := it.guild.get_role(s["partner_role"])) and role in member.roles:
            try:
                await member.remove_roles(role, reason="Partnership ended")
            except discord.HTTPException:
                pass
        e = with_user(pnl("💔 Partnership Removed", f"{member.mention} is no longer a partner.", ERROR), member)
        await it.response.send_message(embed=e)
        await log_to(it.guild, "log_channel", e)

    @group.command(name="info", description="Show a partner record")
    async def info(self, it, member: discord.Member):
        r = await one("SELECT * FROM partners WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not r:
            return await it.response.send_message(embed=inf("Not a registered partner."), ephemeral=True)
        await it.response.send_message(embed=with_user(pnl("🤝 Partner Record",
            f"**Note:** {r['note']}\n**Added by:** <@{r['added_by']}>\n"
            f"**Since:** <t:{int(datetime.fromisoformat(r['created_at']).timestamp())}:F>"), member))

    @group.command(name="list", description="List all partners")
    async def list_(self, it):
        rows = await all_("SELECT * FROM partners WHERE guild_id=? ORDER BY created_at DESC", (it.guild.id,))
        if not rows:
            return await it.response.send_message(embed=inf("No partners yet."), ephemeral=True)
        body = "\n".join(f"• <@{r['user_id']}> — {r['note'][:60]}" for r in rows[:25])
        await it.response.send_message(embed=pnl(f"🤝 Partners ({len(rows)})", body), ephemeral=True)


# ─────────────────────────── invites ───────────────────────────

class Invites(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="invites", description="Show invite counts")
    async def invites(self, it, member: discord.Member | None = None):
        target = member or it.user
        r = await one("SELECT * FROM invites WHERE guild_id=? AND user_id=?", (it.guild.id, target.id))
        joins = r["joins"] if r else 0
        leaves = r["leaves"] if r else 0
        fake = r["fake"] if r else 0
        e = with_user(pnl("📨 Invites", f"**{target.display_name}**\n"
                          f"**Valid:** `{max(0, joins - leaves - fake)}`\n"
                          f"**Joined:** `{joins}` · **Left:** `{leaves}` · **Fake:** `{fake}`"), target)
        await it.response.send_message(embed=e)

    @app_commands.command(name="invite_leaderboard", description="Top inviters")
    async def leaderboard(self, it):
        rows = await all_("SELECT * FROM invites WHERE guild_id=? ORDER BY joins DESC LIMIT 10", (it.guild.id,))
        if not rows:
            return await it.response.send_message(embed=inf("No invite data yet."), ephemeral=True)
        medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
        body = "\n".join(f"{medals[i]} <@{r['user_id']}> — `{max(0, r['joins'] - r['leaves'] - r['fake'])}` valid"
                         for i, r in enumerate(rows))
        await it.response.send_message(embed=pnl("🏆 Invite Leaderboard", body))

    @app_commands.command(name="invite_who", description="Show who invited a member")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def who(self, it, member: discord.Member):
        r = await one("SELECT * FROM invite_joins WHERE guild_id=? AND user_id=? ORDER BY id DESC",
                      (it.guild.id, member.id))
        if not r:
            return await it.response.send_message(embed=inf("No invite record for that member."), ephemeral=True)
        await it.response.send_message(embed=with_user(pnl("🔎 Invite Origin",
            f"Invited by <@{r[0]['inviter_id']}>\n<t:{int(datetime.fromisoformat(r[0]['created_at']).timestamp())}:R>"),
            member))

    @app_commands.command(name="invite_reset", description="Reset a member's invite counts")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def reset(self, it, member: discord.Member):
        await ex("INSERT OR REPLACE INTO invites (guild_id,user_id,joins,leaves,fake) VALUES (?,?,0,0,0)",
                 (it.guild.id, member.id))
        await it.response.send_message(embed=ok(f"Reset invites for {member.mention}."))

    @app_commands.command(name="invite_log", description="Set the invite log channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def setlog(self, it, channel: discord.TextChannel):
        await setg(it.guild.id, invite_log=channel.id)
        await it.response.send_message(embed=ok(f"Invite log → {channel.mention}."))

    @commands.Cog.listener()
    async def on_invite_create(self, invite: discord.Invite):
        await self.bot.cache_invites(invite.guild)

    @commands.Cog.listener()
    async def on_invite_delete(self, invite: discord.Invite):
        await self.bot.cache_invites(invite.guild)

    @commands.Cog.listener()
    async def on_member_join(self, m: discord.Member):
        before = self.bot.invites.get(m.guild.id, {})
        try:
            invites = await m.guild.invites()
        except discord.HTTPException:
            return
        used = None
        for i in invites:
            if i.uses and i.uses > before.get(i.code, 0):
                used = i
                break
        self.bot.invites[m.guild.id] = {i.code: i.uses or 0 for i in invites}
        if used is None or used.inviter is None:
            return
        await ex("INSERT INTO invites (guild_id,user_id,joins) VALUES (?,?,1) "
                 "ON CONFLICT(guild_id,user_id) DO UPDATE SET joins=joins+1",
                 (m.guild.id, used.inviter.id))
        await ex("INSERT INTO invite_joins (guild_id,user_id,inviter_id,created_at) VALUES (?,?,?,?)",
                 (m.guild.id, m.id, used.inviter.id, discord.utils.utcnow().isoformat()))
        s = await settings(m.guild.id)
        if s["invite_log"] and (ch := m.guild.get_channel(s["invite_log"])):
            try:
                await ch.send(embed=with_user(pnl("📨 Member Invited",
                    f"{m.mention} joined via **{used.code}** by {used.inviter.mention}", INFO), m))
            except discord.HTTPException:
                pass

    @commands.Cog.listener()
    async def on_member_remove(self, m: discord.Member):
        r = await one("SELECT * FROM invite_joins WHERE guild_id=? AND user_id=? ORDER BY id DESC",
                      (m.guild.id, m.id))
        if not r:
            return
        inviter = r[0]["inviter_id"]
        age = (discord.utils.utcnow() - datetime.fromisoformat(r[0]["created_at"])).total_seconds()
        if age < 60:
            await ex("INSERT INTO invites (guild_id,user_id,fake) VALUES (?,?,1) "
                     "ON CONFLICT(guild_id,user_id) DO UPDATE SET fake=fake+1", (m.guild.id, inviter))
        else:
            await ex("INSERT INTO invites (guild_id,user_id,leaves) VALUES (?,?,1) "
                     "ON CONFLICT(guild_id,user_id) DO UPDATE SET leaves=leaves+1", (m.guild.id, inviter))


# ─────────────────────────── core ───────────────────────────

class Core(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="setup", description="Setup checklist")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it):
        s = await settings(it.guild.id)
        t = lambda v: "✅" if v else "⬜"
        await it.response.send_message(ephemeral=True, embed=pnl("🧭 Setup Checklist", "\n".join([
            f"{t(s['log_channel'])} `/logs` — log channel",
            f"{t(s['welcome_channel'])} `/welcome` — welcomer",
            f"{t(s['verify_enabled'])} `/verify enable` — verification",
            f"{t(s['signin_channel'])} `/staff_setup` — staff sign-in",
            f"{t(await one('SELECT 1 FROM staff_ranks WHERE guild_id=?', (it.guild.id,)))} `/rank_add` — promotion ladder",
            f"{t(s['voice_hub'])} `/voice-setup` — temp voice hub",
            f"{t(s['ticket_category'])} `/ticket setup` — ticket system",
            f"{t(s['partner_channel'])} `/partner setup` — partnerships",
            f"{t(s['antiraid_enabled'])} `/antiraid` — raid protection",
        ])))

    @app_commands.command(name="help", description="List all commands")
    async def help_(self, it):
        e = pnl("📖 Command Reference", "Every command replies with an embed. Prefix commands exist too.", DEFAULT)
        e.add_field(name="🛡️ Moderation", value="`ban` `unban` `kick` `softban` `timeout` `untimeout` `warn` `warnings` `delwarn` `purge` `slowmode` `lock` `unlock` `nick` `addrole` `removerole`")
        e.add_field(name="🤖 Automod", value="`automod` `blockword` `unblockword` `blockedwords` `blockword_punish` `automod_ignore` `automod_unignore`")
        e.add_field(name="🚨 Anti-Raid", value="`antiraid` `raidmode`")
        e.add_field(name="👋 Welcomer", value="`welcome` `goodbye` `welcome_preview`")
        e.add_field(name="🔐 Verification", value="`verify enable` `verify disable` `verify status` `verify alts` `verify whois`")
        e.add_field(name="🔊 Voice", value="`voice-setup` `voice-clean`")
        e.add_field(name="🎫 Tickets", value="`ticket setup` `ticket add_type` `ticket remove_type` `ticket panel` `ticket close` `ticket claim` `ticket add` `ticket remove` `ticket stats`")
        e.add_field(name="🤝 Partners", value="`partner setup` `partner add` `partner remove` `partner info` `partner list`")
        e.add_field(name="📨 Invites", value="`invites` `invite_leaderboard` `invite_who` `invite_reset` `invite_log`")
        e.add_field(name="🛡️ Staff", value="`staff_setup` `staff` `rank_add` `ranks` `rank_remove` `promote` `demote` `points` `staffstats` `stafftop`")
        await it.response.send_message(embed=e, ephemeral=True)


# ─────────────────────────── prefix ───────────────────────────

class Prefix(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @commands.command(name="ping")
    async def ping(self, ctx):
        await ctx.send(embed=ok(f"Pong — `{round(self.bot.latency * 1000)}ms`"))

    @commands.command(name="ban")
    @commands.has_guild_permissions(ban_members=True)
    async def ban(self, ctx, member: discord.Member, *, reason: str = "No reason given"):
        if why := can_act(ctx.guild, ctx.author, member):
            return await ctx.send(embed=err(why))
        await ctx.send(embed=await act_ban(ctx.guild, ctx.author, member, reason))

    @commands.command(name="unban")
    @commands.has_guild_permissions(ban_members=True)
    async def unban(self, ctx, user_id: int, *, reason: str = "No reason given"):
        try:
            u = await self.bot.fetch_user(user_id)
            await ctx.send(embed=await act_unban(ctx.guild, ctx.author, u, reason))
        except (discord.NotFound, discord.HTTPException):
            await ctx.send(embed=err("User not found or not banned."))

    @commands.command(name="kick")
    @commands.has_guild_permissions(kick_members=True)
    async def kick(self, ctx, member: discord.Member, *, reason: str = "No reason given"):
        if why := can_act(ctx.guild, ctx.author, member):
            return await ctx.send(embed=err(why))
        await ctx.send(embed=await act_kick(ctx.guild, ctx.author, member, reason))

    @commands.command(name="softban")
    @commands.has_guild_permissions(ban_members=True)
    async def softban(self, ctx, member: discord.Member, *, reason: str = "No reason given"):
        if why := can_act(ctx.guild, ctx.author, member):
            return await ctx.send(embed=err(why))
        await ctx.send(embed=await act_softban(ctx.guild, ctx.author, member, reason))

    @commands.command(name="timeout", aliases=["mute"])
    @commands.has_guild_permissions(moderate_members=True)
    async def timeout(self, ctx, member: discord.Member, minutes: int, *, reason: str = "No reason given"):
        if why := can_act(ctx.guild, ctx.author, member):
            return await ctx.send(embed=err(why))
        await ctx.send(embed=await act_timeout(ctx.guild, ctx.author, member, minutes, reason))

    @commands.command(name="untimeout", aliases=["unmute"])
    @commands.has_guild_permissions(moderate_members=True)
    async def untimeout(self, ctx, member: discord.Member):
        await member.timeout(None, reason=str(ctx.author))
        await ctx.send(embed=ok(f"{member.mention} unmuted."))

    @commands.command(name="warn")
    @commands.has_guild_permissions(manage_messages=True)
    async def warn(self, ctx, member: discord.Member, *, reason: str):
        await ctx.send(embed=await act_warn(ctx.guild, ctx.author, member, reason))

    @commands.command(name="warnings")
    async def warnings(self, ctx, member: discord.Member):
        rows = await all_("SELECT * FROM warnings WHERE guild_id=? AND user_id=? ORDER BY id DESC",
                          (ctx.guild.id, member.id))
        if not rows:
            return await ctx.send(embed=inf(f"{member.mention} has no warnings."))
        body = "\n".join(f"`#{r['id']}` {r['reason']} — <@{r['mod_id']}>" for r in rows[:15])
        await ctx.send(embed=with_user(pnl(f"📋 {len(rows)} warning(s)", body, WARN), member))

    @commands.command(name="delwarn")
    @commands.has_guild_permissions(manage_messages=True)
    async def delwarn(self, ctx, warning_id: int):
        cur = await ex("DELETE FROM warnings WHERE id=? AND guild_id=?", (warning_id, ctx.guild.id))
        if cur.rowcount == 0:
            return await ctx.send(embed=err("No such warning."))
        await ctx.send(embed=ok(f"Warning `#{warning_id}` deleted."))

    @commands.command(name="purge", aliases=["clear"])
    @commands.has_guild_permissions(manage_messages=True)
    async def purge(self, ctx, amount: int = 10, member: discord.Member | None = None):
        try:
            await ctx.message.delete()
        except discord.HTTPException:
            pass
        d = await ctx.channel.purge(limit=amount, check=(lambda m: m.author.id == member.id) if member else None)
        await ctx.send(embed=ok(f"Deleted **{len(d)}** message(s)."), delete_after=5)

    @commands.command(name="slowmode")
    @commands.has_guild_permissions(manage_channels=True)
    async def slowmode(self, ctx, seconds: int):
        await ctx.channel.edit(slowmode_delay=seconds)
        await ctx.send(embed=ok(f"Slowmode **{seconds}s**."))

    @commands.command(name="lock")
    @commands.has_guild_permissions(manage_channels=True)
    async def lock(self, ctx, channel: discord.TextChannel | None = None):
        c = channel or ctx.channel
        ow = c.overwrites_for(ctx.guild.default_role)
        ow.send_messages = False
        await c.set_permissions(ctx.guild.default_role, overwrite=ow)
        await ctx.send(embed=ok(f"🔒 {c.mention} locked."))

    @commands.command(name="unlock")
    @commands.has_guild_permissions(manage_channels=True)
    async def unlock(self, ctx, channel: discord.TextChannel | None = None):
        c = channel or ctx.channel
        ow = c.overwrites_for(ctx.guild.default_role)
        ow.send_messages = None
        await c.set_permissions(ctx.guild.default_role, overwrite=ow)
        await ctx.send(embed=ok(f"🔓 {c.mention} unlocked."))

    @commands.command(name="nick")
    @commands.has_guild_permissions(manage_nicknames=True)
    async def nick(self, ctx, member: discord.Member, *, nickname: str | None = None):
        await member.edit(nick=nickname)
        await ctx.send(embed=ok(f"Nickname updated for {member.mention}."))

    @commands.command(name="addrole")
    @commands.has_guild_permissions(manage_roles=True)
    async def addrole(self, ctx, member: discord.Member, role: discord.Role):
        if role >= ctx.guild.me.top_role:
            return await ctx.send(embed=err("That role is above mine."))
        await member.add_roles(role, reason=str(ctx.author))
        await ctx.send(embed=ok(f"Gave {role.mention} to {member.mention}."))

    @commands.command(name="removerole")
    @commands.has_guild_permissions(manage_roles=True)
    async def removerole(self, ctx, member: discord.Member, role: discord.Role):
        await member.remove_roles(role, reason=str(ctx.author))
        await ctx.send(embed=ok(f"Removed {role.mention} from {member.mention}."))

    @commands.command(name="blockword")
    @commands.has_guild_permissions(manage_guild=True)
    async def blockword(self, ctx, *, word: str):
        await ex("INSERT OR IGNORE INTO blocked_words VALUES (?,?)", (ctx.guild.id, word.lower()))
        await ctx.send(embed=ok(f"Blocked `{word}`."))

    @commands.command(name="unblockword")
    @commands.has_guild_permissions(manage_guild=True)
    async def unblockword(self, ctx, *, word: str):
        await ex("DELETE FROM blocked_words WHERE guild_id=? AND word=?", (ctx.guild.id, word.lower()))
        await ctx.send(embed=ok(f"Unblocked `{word}`."))

    @commands.command(name="blockpunish")
    @commands.has_guild_permissions(manage_guild=True)
    async def blockpunish(self, ctx, action: str, minutes: int = 10):
        if action not in ("delete", "warn", "timeout", "kick", "ban"):
            return await ctx.send(embed=err("Use delete, warn, timeout, kick or ban."))
        await setg(ctx.guild.id, block_punish=action, block_punish_minutes=minutes)
        await ctx.send(embed=ok(f"Blocked-word punishment → **{action}**."))

    @commands.command(name="automod")
    @commands.has_guild_permissions(manage_guild=True)
    async def automod(self, ctx, enabled: bool):
        await setg(ctx.guild.id, automod_enabled=int(enabled))
        await ctx.send(embed=ok(f"Automod {'enabled' if enabled else 'disabled'}."))

    @commands.command(name="antiraid")
    @commands.has_guild_permissions(administrator=True)
    async def antiraid(self, ctx, enabled: bool, threshold: int = 8, window: int = 12, action: str = "alert"):
        await setg(ctx.guild.id, antiraid_enabled=int(enabled), antiraid_threshold=threshold,
                   antiraid_window=window, antiraid_action=action)
        await ctx.send(embed=ok(f"Anti-raid {'on' if enabled else 'off'} — {threshold}/{window}s → `{action}`."))

    @commands.command(name="raidmode")
    @commands.has_guild_permissions(administrator=True)
    async def raidmode(self, ctx, enabled: bool):
        if enabled:
            self.bot.raid_mode[ctx.guild.id] = discord.utils.utcnow().timestamp()
            await ctx.guild.edit(verification_level=discord.VerificationLevel.high)
        else:
            self.bot.raid_mode.pop(ctx.guild.id, None)
        await ctx.send(embed=ok(f"Raid mode {'ENABLED' if enabled else 'disabled'}."))

    @commands.command(name="welcome")
    @commands.has_guild_permissions(manage_guild=True)
    async def welcome(self, ctx, channel: discord.TextChannel, *,
                      message: str = "Hey {user}, welcome to **{server}**! You're member **#{count}**."):
        await setg(ctx.guild.id, welcome_channel=channel.id, welcome_message=message)
        self.bot.add_view(WelcomeView(ctx.guild.id))
        await ctx.send(embed=ok(f"Welcomer set to {channel.mention}."))

    @commands.command(name="goodbye")
    @commands.has_guild_permissions(manage_guild=True)
    async def goodbye(self, ctx, channel: discord.TextChannel, *, message: str = "👋 **{name}** left."):
        await setg(ctx.guild.id, goodbye_channel=channel.id, goodbye_message=message)
        await ctx.send(embed=ok(f"Goodbye set to {channel.mention}."))

    @commands.group(name="verify", invoke_without_command=True)
    async def verify(self, ctx):
        s = await settings(ctx.guild.id)
        await ctx.send(embed=pnl("🔐 Verification Status",
                                 f"**Enabled:** {'🟢' if s['verify_enabled'] else '🔴'}\n"
                                 f"**Web URL:** `{WEB_URL or 'not set'}`\n\n`!verify enable #channel @role`"))

    @verify.command(name="enable")
    @commands.has_guild_permissions(administrator=True)
    async def verify_enable(self, ctx, channel: discord.TextChannel, role: discord.Role,
                            log_channel: discord.TextChannel | None = None):
        if not link_ok():
            return await ctx.send(embed=err("`WEB_URL` is not configured."))
        if role >= ctx.guild.me.top_role:
            return await ctx.send(embed=err("That role is above mine."))
        await setg(ctx.guild.id, verify_enabled=1, verify_role=role.id,
                   verify_log=log_channel.id if log_channel else None)
        self.bot.add_view(VerifyView(ctx.guild.id))
        e = pnl("🔐 Verification Required", "Click the button below to verify through our website.")
        e.add_field(name="Steps", value="1️⃣ Click **Verify**\n2️⃣ Authorize\n3️⃣ You're in!")
        await channel.send(embed=e, view=VerifyView(ctx.guild.id))
        await ctx.send(embed=ok(f"Verification enabled in {channel.mention}."))

    @verify.command(name="disable")
    @commands.has_guild_permissions(administrator=True)
    async def verify_disable(self, ctx):
        await setg(ctx.guild.id, verify_enabled=0)
        await ctx.send(embed=ok("Verification disabled."))

    @commands.command(name="whois")
    @commands.has_guild_permissions(manage_guild=True)
    async def whois(self, ctx, member: discord.Member):
        v = await one("SELECT * FROM verifications WHERE guild_id=? AND user_id=?", (ctx.guild.id, member.id))
        if not v:
            return await ctx.send(embed=inf("No record."))
        await ctx.send(embed=with_user(pnl("🕵️ Record", f"**IP:** `{v['ip']}`\n**UA:** `{v['user_agent'][:70]}`"), member))

    @commands.command(name="logs")
    @commands.has_guild_permissions(manage_guild=True)
    async def logs(self, ctx, channel: discord.TextChannel, kind: str = "all"):
        m = {"all": "log_channel", "mod": "mod_log", "join": "join_log", "verify": "verify_log"}
        if kind not in m:
            return await ctx.send(embed=err("Use all, mod, join or verify."))
        await setg(ctx.guild.id, **{m[kind]: channel.id})
        await ctx.send(embed=ok(f"`{kind}` logs → {channel.mention}."))

    @commands.command(name="staffsetup")
    @commands.has_guild_permissions(administrator=True)
    async def staffsetup(self, ctx, channel: discord.TextChannel, signin_word: str, signout_word: str = "signout"):
        await setg(ctx.guild.id, signin_channel=channel.id,
                   signin_word=signin_word.lower(), signout_word=signout_word.lower())
        await channel.send(embed=pnl("🛡️ Staff Duty Channel",
            f"Type **{signin_word}** to start your shift, **{signout_word}** to end it.\n"
            "1 hour on duty = +5 points · 20 messages/day = +5 points · idle 1h = auto sign-out · 24h inactive = −20 points."))
        await ctx.send(embed=ok(f"Set in {channel.mention} with word `{signin_word}`."))

    @commands.command(name="signin")
    async def signin(self, ctx):
        _, _, emb = await staff_signin(ctx.guild, ctx.author)
        await ctx.send(embed=emb)

    @commands.command(name="signout")
    async def signout(self, ctx):
        _, _, emb = await staff_signout(ctx.guild, ctx.author, "word")
        await ctx.send(embed=emb)

    @commands.command(name="rankadd")
    @commands.has_guild_permissions(administrator=True)
    async def rankadd(self, ctx, name: str, points: int, role: discord.Role):
        n = (await one("SELECT COUNT(*) c FROM staff_ranks WHERE guild_id=?", (ctx.guild.id,)))["c"]
        await ex("INSERT INTO staff_ranks (guild_id,name,threshold,role_id,position) VALUES (?,?,?,?,?)",
                 (ctx.guild.id, name, points, role.id, n))
        await ctx.send(embed=ok(f"Rank **{name}** at `{points}` → {role.mention}."))

    @commands.command(name="promote")
    @commands.has_guild_permissions(manage_guild=True)
    async def promote(self, ctx, member: discord.Member, *, rank: str):
        r = await one("SELECT * FROM staff_ranks WHERE guild_id=? AND name=?", (ctx.guild.id, rank))
        if not r:
            return await ctx.send(embed=err("Unknown rank."))
        role = ctx.guild.get_role(r["role_id"])
        if not role:
            return await ctx.send(embed=err("Rank role missing."))
        await member.add_roles(role, reason=f"Manual promotion by {ctx.author}")
        e = with_user(pnl("📈 Promotion", f"{member.mention} → **{rank}**.", SUCCESS), member)
        await ctx.send(embed=e)
        await log_to(ctx.guild, "log_channel", e)

    @commands.command(name="points")
    @commands.has_guild_permissions(manage_guild=True)
    async def points(self, ctx, member: discord.Member, amount: int, *, reason: str = "Manual adjustment"):
        await award_points(ctx.guild, member, amount, f"{reason} (by {ctx.author})")
        r = await ensure_staff(ctx.guild.id, member.id)
        await ctx.send(embed=ok(f"{member.mention} now has `{r['points']}` points."))

    @commands.command(name="invites")
    async def invites(self, ctx, member: discord.Member | None = None):
        target = member or ctx.author
        r = await one("SELECT * FROM invites WHERE guild_id=? AND user_id=?", (ctx.guild.id, target.id))
        joins = r["joins"] if r else 0
        leaves = r["leaves"] if r else 0
        fake = r["fake"] if r else 0
        await ctx.send(embed=with_user(pnl("📨 Invites", f"**Valid:** `{max(0, joins - leaves - fake)}`\n"
                                           f"**Joined:** `{joins}` · **Left:** `{leaves}` · **Fake:** `{fake}`"), target))

    @commands.command(name="help")
    async def help_(self, ctx):
        e = pnl("📖 Command Reference", f"Prefix: `{PREFIX}` — slash commands also available.", DEFAULT)
        e.add_field(name="🛡️ Moderation", value="`ban` `unban` `kick` `softban` `timeout` `untimeout` `warn` `warnings` `delwarn` `purge` `slowmode` `lock` `unlock` `nick` `addrole` `removerole`")
        e.add_field(name="🤖 Automod", value="`automod` `blockword` `unblockword` `blockpunish`")
        e.add_field(name="🚨 Anti-Raid", value="`antiraid` `raidmode`")
        e.add_field(name="👋 Welcomer", value="`welcome` `goodbye`")
        e.add_field(name="🔐 Verification", value="`verify` `verify enable` `verify disable` `whois`")
        e.add_field(name="📨 Invites", value="`invites`")
        e.add_field(name="🛡️ Staff", value="`staffsetup` `signin` `signout` `rankadd` `promote` `points`")
        await ctx.send(embed=e)


# ─────────────────────────── website ───────────────────────────

PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title }}</title><style>
:root{color-scheme:dark}body{margin:0;min-height:100vh;display:grid;place-items:center;
background:#0b0c10;color:#e8e8ec;font:16px/1.6 system-ui,sans-serif}
.card{max-width:440px;padding:40px;border-radius:18px;background:#14161c;border:1px solid #22252e;text-align:center}
h1{margin:0 0 12px;font-size:22px}.emoji{font-size:44px;display:block;margin-bottom:14px}
p{color:#a2a6b3;margin:0 0 20px}a.btn{display:inline-block;padding:12px 26px;border-radius:10px;background:#5865f2;color:#fff;text-decoration:none;font-weight:600}
.meta{margin-top:18px;font-size:13px;color:#6b7080}</style></head><body><div class="card">
<span class="emoji">{{ emoji }}</span><h1>{{ title }}</h1><p>{{ body }}</p>{{ extra|safe }}
<div class="meta">Venice Verification</div></div></body></html>"""

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
signer = URLSafeTimedSerializer(SECRET_KEY)


def page(title, body, emoji="🔐", extra=""):
    return render_template_string(PAGE, title=title, body=body, emoji=emoji, extra=extra)


def run_async(coro, timeout=30):
    if bot.loop is None or bot.loop.is_closed():
        raise RuntimeError("Bot loop not ready")
    return asyncio.run_coroutine_threadsafe(coro, bot.loop).result(timeout)


@app.get("/")
def index():
    return page("Verification Gateway", "Add the bot and run /verify enable in your server to get started.", "🛡️")


@app.get("/health")
def health():
    return {"ok": True, "ready": bot.is_ready(), "guilds": len(bot.guilds)}


@app.get("/verify")
def verify_route():
    gid = request.args.get("g")
    if not gid or not gid.isdigit():
        return page("Invalid Link", "This verification link is missing a server ID.", "❌"), 400
    state = signer.dumps({"g": int(gid)})
    url = (f"{API}/oauth2/authorize?client_id={CLIENT_ID}&response_type=code"
           f"&redirect_uri={REDIRECT_URI}&scope=identify%20guilds.join&state={state}&prompt=consent")
    return redirect(url)


@app.get("/callback")
def callback():
    code, state = request.args.get("code"), request.args.get("state")
    if not code or not state:
        return page("Cancelled", "You denied the authorization request.", "🚫"), 400
    try:
        gid = signer.loads(state, max_age=600)["g"]
    except Exception:
        return page("Expired", "This link expired. Click the button again.", "⌛"), 400

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "unknown").split(",")[0].strip()
    ua = request.headers.get("User-Agent", "unknown")[:250]

    tok = requests.post(f"{API}/oauth2/token", data={
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "grant_type": "authorization_code",
        "code": code, "redirect_uri": REDIRECT_URI,
    }, timeout=15)
    if tok.status_code != 200:
        log.warning("token exchange failed: %s", tok.text[:300])
        return page("Authorization Failed", "Discord rejected the code. Try again.", "❌"), 400

    me = requests.get(f"{API}/users/@me",
                      headers={"Authorization": f"Bearer {tok.json()['access_token']}"}, timeout=15)
    if me.status_code != 200:
        return page("Profile Error", "Could not read your Discord profile.", "❌"), 400
    user = me.json()

    try:
        good, msg = run_async(handle(gid, user, ip, ua))
    except Exception as e:
        log.exception("verification failed")
        return page("Server Error", str(e)[:200], "⚠️"), 500

    if not good:
        return page("Could Not Verify", msg, "⚠️"), 400
    if msg == "already":
        return page("Already Verified", f"You're verified in that server, {user['username']}.", "✅")
    return page("Verified", f"Welcome, {user['username']}. Head back to Discord — your roles are applied.", "✅",
                '<a class="btn" href="https://discord.com/channels/">Return to Discord</a>')


async def handle(gid: int, user: dict, ip: str, ua: str):
    guild = bot.get_guild(gid)
    if guild is None:
        return False, "The bot isn't in that server anymore."

    s = await settings(gid)
    if not s["verify_enabled"]:
        return False, "Verification isn't enabled in that server."

    if await one("SELECT 1 FROM verifications WHERE guild_id=? AND user_id=?", (gid, user["id"])):
        return True, "already"

    now = datetime.now(timezone.utc)
    try:
        member = guild.get_member(user["id"]) or await guild.fetch_member(user["id"])
    except discord.NotFound:
        return False, "You need to join the server before verifying."

    await ex("INSERT INTO verifications (guild_id,user_id,ip,user_agent,verified_at) VALUES (?,?,?,?,?)",
             (gid, user["id"], ip, ua, now.isoformat()))

    alts = await all_("SELECT user_id FROM verifications WHERE guild_id=? AND ip=? AND user_id!=?",
                      (gid, ip, user["id"]))
    for a in alts:
        await ex("INSERT INTO alts (guild_id,user_id,matched_user_id,reason,created_at) VALUES (?,?,?,?,?)",
                 (gid, user["id"], a["user_id"], f"same IP ({ip})", now.isoformat()))

    try:
        if s["verify_role"] and (role := guild.get_role(s["verify_role"])):
            await member.add_roles(role, reason="Verification")
        if s["unverified_role"] and (role := guild.get_role(s["unverified_role"])) and role in member.roles:
            await member.remove_roles(role, reason="Verification")
    except discord.HTTPException as e:
        log.warning("role update failed: %s", e)

    if s["verify_log"] and (ch := guild.get_channel(s["verify_log"])):
        created = datetime.fromtimestamp(((int(user["id"]) >> 22) + 1420070400000) / 1000, tz=timezone.utc)
        age = (now - created).days
        avatar = (f"https://cdn.discordapp.com/avatars/{user['id']}/{user['avatar']}.png"
                  if user.get("avatar") else "https://cdn.discordapp.com/embed/avatars/0.png")
        e = pnl("🔐 Member Verified", f"<@{user['id']}> verified.", SUCCESS)
        e.add_field(name="IP", value=f"`{ip}`", inline=True)
        e.add_field(name="Account age", value=f"`{age}d`", inline=True)
        if alts:
            e.add_field(name="Possible alts", value="\n".join(f"<@{a['user_id']}>" for a in alts[:10]), inline=False)
        if age < 7:
            e.add_field(name="⚠️ Note", value="Account younger than 7 days.", inline=False)
        e.set_thumbnail(url=avatar)
        try:
            await ch.send(embed=e)
        except discord.HTTPException:
            pass

    return True, "ok"


def keep_alive():
    url = os.getenv("RENDER_EXTERNAL_URL")
    if not url:
        return
    while True:
        time.sleep(600)
        try:
            requests.get(f"{url}/health", timeout=20)
        except Exception:
            pass


# ─────────────────────────── entrypoint ───────────────────────────

if __name__ == "__main__":
    missing = [k for k in ("TOKEN", "CLIENT_ID", "CLIENT_SECRET", "SECRET_KEY", "WEB_URL", "REDIRECT_URI")
               if not os.getenv(k)]
    if missing:
        log.error("Missing environment variables: %s", ", ".join(missing))
        raise SystemExit(1)

    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True),
                     daemon=True).start()
    log.info("Web server bound to :%s", PORT)
    threading.Thread(target=keep_alive, daemon=True).start()

    try:
        bot.run(TOKEN, log_handler=None)
    except Exception:
        log.exception("Bot crashed on startup")
        raise SystemExit(1)pass
