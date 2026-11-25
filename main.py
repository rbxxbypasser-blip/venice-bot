import asyncio
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
from discord.ext import commands
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
PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "8080")))
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
    antiraid_enabled INTEGER DEFAULT 1, antiraid_threshold INTEGER DEFAULT 8,
    antiraid_window INTEGER DEFAULT 12, antiraid_action TEXT DEFAULT 'alert',
    min_account_age INTEGER DEFAULT 0, automod_enabled INTEGER DEFAULT 1, automod_escalation INTEGER DEFAULT 3
);
CREATE TABLE IF NOT EXISTS blocked_words (guild_id INTEGER, word TEXT, PRIMARY KEY (guild_id, word));
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
"""


async def db_init():
    global _conn
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    _conn = await aiosqlite.connect(DB_PATH)
    _conn.row_factory = aiosqlite.Row
    await _conn.executescript(SCHEMA)
    await _conn.commit()


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
        await ex("INSERT INTO guilds (guild_id) VALUES (?)", (gid,))
        r = await one("SELECT * FROM guilds WHERE guild_id=?", (gid,))
    return dict(r)


async def setg(gid: int, **f):
    if not f:
        return
    await ex(f"UPDATE guilds SET {', '.join(k + ' = ?' for k in f)} WHERE guild_id=?", (*f.values(), gid))


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


# ─────────────────────────── views ───────────────────────────

class VerifyView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Verify", emoji="🔐", style=discord.ButtonStyle.link, url=f"{WEB_URL}/verify?g={gid}"))


class WelcomeView(discord.ui.View):
    def __init__(self, gid: int, rules: str | None = None):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(label="Verify", emoji="🔐", style=discord.ButtonStyle.link, url=f"{WEB_URL}/verify?g={gid}"))
        if rules:
            self.add_item(discord.ui.Button(label="Rules", emoji="📜", style=discord.ButtonStyle.link, url=rules))

    @discord.ui.button(label="Say hi", emoji="👋", style=discord.ButtonStyle.secondary, custom_id="wv:hi", row=1)
    async def hi(self, it: discord.Interaction, b: discord.ui.Button):
        await it.response.send_message(embed=inf(f"Welcome to **{it.guild.name}**, {it.user.mention}! Glad you're here. 🎉"), ephemeral=True)


class StaffPanelView(discord.ui.View):
    def __init__(self, gid: int):
        super().__init__(timeout=None)
        self.gid = gid

    async def _row(self, uid):
        r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (self.gid, uid))
        if r is None:
            await ex("INSERT INTO staff (guild_id, user_id) VALUES (?,?)", (self.gid, uid))
            r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (self.gid, uid))
        return r

    @discord.ui.button(label="Sign In", emoji="🟢", style=discord.ButtonStyle.success, custom_id="duty:in")
    async def sign_in(self, it: discord.Interaction, b: discord.ui.Button):
        r = await self._row(it.user.id)
        now = discord.utils.utcnow()
        if r["on_duty"]:
            return await it.response.send_message(embed=wrn(f"Already on duty since <t:{int(datetime.fromisoformat(r['duty_start']).timestamp())}:t>."), ephemeral=True)
        if r["last_signin"] == now.date().isoformat():
            return await it.response.send_message(embed=wrn("You already completed your shift today. Come back tomorrow. 📅"), ephemeral=True)
        days = r["days_present"] + 1
        await ex("UPDATE staff SET on_duty=1, duty_start=?, last_signin=?, days_present=? WHERE guild_id=? AND user_id=?",
                 (now.isoformat(), now.date().isoformat(), days, self.gid, it.user.id))
        e = with_user(pnl("🟢 Staff On Duty", f"{it.user.mention} signed in.\n**Time:** <t:{int(now.timestamp())}:T>\n**Days present:** `{days}`", SUCCESS), it.user)
        e.add_field(name="Role", value=it.user.top_role.mention)
        e.set_footer(text=f"ID {it.user.id}")
        await it.response.send_message(embed=e)

    @discord.ui.button(label="Sign Out", emoji="🔴", style=discord.ButtonStyle.danger, custom_id="duty:out")
    async def sign_out(self, it: discord.Interaction, b: discord.ui.Button):
        r = await self._row(it.user.id)
        if not r["on_duty"]:
            return await it.response.send_message(embed=wrn("You're not signed in."), ephemeral=True)
        now = discord.utils.utcnow()
        start = datetime.fromisoformat(r["duty_start"])
        secs = max(1, int((now - start).total_seconds()))
        pts = r["points"] + secs // 600
        await ex("UPDATE staff SET on_duty=0, duty_start=NULL, total_seconds=total_seconds+?, points=? WHERE guild_id=? AND user_id=?",
                 (secs, pts, self.gid, it.user.id))
        await ex("INSERT INTO duty (guild_id, user_id, start_ts, end_ts, seconds) VALUES (?,?,?,?,?)",
                 (self.gid, it.user.id, start.isoformat(), now.isoformat(), secs))
        h, m = secs // 3600, (secs % 3600) // 60
        e = with_user(pnl("🔴 Staff Off Duty", f"{it.user.mention} signed out.\n**Shift:** `{h}h {m}m`\n**Days present:** `{r['days_present']}`\n**Points:** `{pts}`", ERROR), it.user)
        await it.response.send_message(embed=e)
        await promote_check(it.guild, it.user)

    @discord.ui.button(label="My Stats", emoji="📊", style=discord.ButtonStyle.secondary, custom_id="duty:stats")
    async def stats(self, it: discord.Interaction, b: discord.ui.Button):
        r = await self._row(it.user.id)
        h, m = r["total_seconds"] // 3600, (r["total_seconds"] % 3600) // 60
        e = with_user(pnl("📊 Your Activity", f"**Points:** `{r['points']}`\n**Days present:** `{r['days_present']}`\n**Total duty time:** `{h}h {m}m`\n**On duty:** {'🟢 Yes' if r['on_duty'] else '🔴 No'}"), it.user)
        await it.response.send_message(embed=e, ephemeral=True)


async def promote_check(guild: discord.Guild, member: discord.Member):
    r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (guild.id, member.id))
    if not r:
        return
    ranks = await all_("SELECT * FROM staff_ranks WHERE guild_id=? ORDER BY threshold ASC", (guild.id,))
    current = [x for x in ranks if x["threshold"] <= r["points"]]
    if not current:
        return
    target = current[-1]
    role = guild.get_role(target["role_id"])
    if role and role not in member.roles:
        try:
            await member.add_roles(role, reason=f"Promotion — {r['points']} points")
        except discord.HTTPException:
            return
        s = await settings(guild.id)
        ch = guild.get_channel(s["log_channel"]) if s["log_channel"] else None
        if ch:
            await ch.send(embed=with_user(pnl("📈 Promotion", f"{member.mention} promoted to **{target['name']}** (`{r['points']}` points).", SUCCESS), member))


# ─────────────────────────── bot ───────────────────────────

class Bot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix=commands.when_mentioned_or(PREFIX), intents=discord.Intents.all(),
                         help_command=None, allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, replied_user=True))
        self.join_cache: dict[int, deque] = defaultdict(lambda: deque(maxlen=50))
        self.raid_mode: dict[int, float] = {}
        self.spam: dict[tuple, deque] = defaultdict(lambda: deque(maxlen=6))

    async def setup_hook(self):
        await db_init()
        for c in (Moderation, AutoMod, AntiRaid, Welcomer, Verification, Logs, Staff, Core):
            await self.add_cog(c(self))
        await self.restore()
        await self.tree.sync()
        log.info("Commands synced")

    async def restore(self):
        for row in await all_("SELECT * FROM guilds"):
            g = dict(row)
            if g["verify_enabled"]:
                self.add_view(VerifyView(g["guild_id"]))
            if g["welcome_channel"]:
                self.add_view(WelcomeView(g["guild_id"]))
            if g["staff_channel"]:
                self.add_view(StaffPanelView(g["guild_id"]))

    async def on_ready(self):
        log.info("Ready as %s — %s guilds", self.user, len(self.guilds))
        await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="the gates 👀"))

    async def on_guild_join(self, g: discord.Guild):
        await settings(g.id)
        if g.system_channel and g.system_channel.permissions_for(g.me).send_messages:
            await g.system_channel.send(embed=pnl("👋 Thanks for adding me!", "Run `/setup` for a checklist and `/help` for all commands."))


bot = Bot()


async def log_to(guild, key: str, embed: discord.Embed):
    s = await settings(guild.id)
    cid = s.get(key) or s.get("log_channel")
    ch = guild.get_channel(cid) if cid else None
    if ch:
        try:
            await ch.send(embed=embed)
        except discord.HTTPException:
            pass


# ─────────────────────────── moderation ───────────────────────────

def mod_check():
    async def p(it: discord.Interaction):
        pm = it.user.guild_permissions
        if pm.administrator or pm.ban_members or pm.kick_members or pm.manage_messages:
            return True
        raise app_commands.CheckFailure("You need moderator permissions.")
    return app_commands.check(p)


class Moderation(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="ban", description="Ban a member")
    @mod_check()
    async def ban(self, it, member: discord.Member, reason: str = "No reason given", delete_days: app_commands.Range[int, 0, 7] = 0):
        await it.response.defer()
        try: await member.send(embed=wrn(f"You were banned from **{it.guild.name}**.\n**Reason:** {reason}", "🔨 Banned"))
        except discord.HTTPException: pass
        await it.guild.ban(member, reason=f"{it.user} — {reason}", delete_message_days=delete_days)
        e = with_user(err(f"{member.mention} banned.\n**Reason:** {reason}\n**Mod:** {it.user.mention}", "🔨 Member Banned"), member)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="unban", description="Unban by user ID")
    @mod_check()
    async def unban(self, it, user_id: str, reason: str = "No reason given"):
        await it.response.defer()
        try:
            u = await self.bot.fetch_user(int(user_id)); await it.guild.unban(u, reason=reason)
        except Exception:
            return await it.followup.send(embed=err("User not found or not banned."))
        e = with_user(ok(f"{u.mention} unbanned.\n**Reason:** {reason}", "🔓 Unbanned"), u)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="kick", description="Kick a member")
    @mod_check()
    async def kick(self, it, member: discord.Member, reason: str = "No reason given"):
        await it.response.defer()
        try: await member.send(embed=wrn(f"You were kicked from **{it.guild.name}**.\n**Reason:** {reason}", "👢 Kicked"))
        except discord.HTTPException: pass
        await member.kick(reason=f"{it.user} — {reason}")
        e = with_user(err(f"{member.mention} kicked.\n**Reason:** {reason}", "👢 Kicked"), member)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="softban", description="Ban + instant unban to purge messages")
    @mod_check()
    async def softban(self, it, member: discord.Member, reason: str = "No reason given"):
        await it.response.defer()
        await it.guild.ban(member, reason=f"Softban — {reason}", delete_message_days=7)
        await it.guild.unban(member, reason="Softban")
        e = with_user(err(f"{member.mention} softbanned.\n**Reason:** {reason}", "🧹 Softbanned"), member)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="timeout", description="Time out a member")
    @mod_check()
    async def timeout(self, it, member: discord.Member, minutes: app_commands.Range[int, 1, 40320], reason: str = "No reason given"):
        await it.response.defer()
        until = discord.utils.utcnow() + timedelta(minutes=minutes)
        await member.timeout(until, reason=reason)
        e = with_user(wrn(f"{member.mention} muted **{minutes}m**.\n**Ends:** <t:{int(until.timestamp())}:R>\n**Reason:** {reason}", "🔇 Timed Out"), member)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="untimeout", description="Remove a timeout")
    @mod_check()
    async def untimeout(self, it, member: discord.Member):
        await it.response.defer(); await member.timeout(None, reason=str(it.user))
        await it.followup.send(embed=ok(f"{member.mention} unmuted."))

    @app_commands.command(name="warn", description="Warn a member")
    @mod_check()
    async def warn(self, it, member: discord.Member, reason: str):
        await it.response.defer()
        await ex("INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES (?,?,?,?,?)",
                 (it.guild.id, member.id, it.user.id, reason, discord.utils.utcnow().isoformat()))
        n = (await one("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?", (it.guild.id, member.id)))["c"]
        try: await member.send(embed=wrn(f"You were warned in **{it.guild.name}**.\n**Reason:** {reason}\n**Total:** {n}", "⚠️ Warning"))
        except discord.HTTPException: pass
        e = with_user(wrn(f"{member.mention} warned (`#{n}`).\n**Reason:** {reason}", "⚠️ Warned"), member)
        await it.followup.send(embed=e); await log_to(it.guild, "mod_log", e)

    @app_commands.command(name="warnings", description="List a member's warnings")
    async def warnings(self, it, member: discord.Member):
        await it.response.defer()
        rows = await all_("SELECT * FROM warnings WHERE guild_id=? AND user_id=? ORDER BY id DESC", (it.guild.id, member.id))
        if not rows: return await it.followup.send(embed=inf(f"{member.mention} has no warnings."))
        body = "\n".join(f"`#{r['id']}` {r['reason']} — <@{r['mod_id']}>" for r in rows[:15])
        await it.followup.send(embed=with_user(pnl(f"📋 {len(rows)} warning(s)", body, WARN), member))

    @app_commands.command(name="delwarn", description="Delete a warning by ID")
    @mod_check()
    async def delwarn(self, it, warning_id: int):
        await it.response.defer()
        await ex("DELETE FROM warnings WHERE id=? AND guild_id=?", (warning_id, it.guild.id))
        await it.followup.send(embed=ok(f"Warning `#{warning_id}` deleted."))

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
        await it.response.send_message(embed=ok(f"Slowmode **{seconds}s**."))

    @app_commands.command(name="lock", description="Lock a channel")
    @mod_check()
    async def lock(self, it, channel: discord.TextChannel | None = None):
        c = channel or it.channel
        ow = c.overwrites_for(it.guild.default_role); ow.send_messages = False
        await c.set_permissions(it.guild.default_role, overwrite=ow)
        await it.response.send_message(embed=ok(f"🔒 {c.mention} locked."))

    @app_commands.command(name="unlock", description="Unlock a channel")
    @mod_check()
    async def unlock(self, it, channel: discord.TextChannel | None = None):
        c = channel or it.channel
        ow = c.overwrites_for(it.guild.default_role); ow.send_messages = None
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
        if not rows: return await it.response.send_message(embed=inf("No blocked words."))
        await it.response.send_message(embed=pnl("🚫 Blocked Words", "`" + "`, `".join(r["word"] for r in rows) + "`"))

    @commands.Cog.listener()
    async def on_message(self, m: discord.Message):
        if not m.guild or m.author.bot or not m.content:
            return
        s = await settings(m.guild.id)
        if not s["automod_enabled"] or m.author.guild_permissions.manage_messages:
            return
        low = m.content.lower()
        hits = [r["word"] for r in await all_("SELECT word FROM blocked_words WHERE guild_id=?", (m.guild.id,)) if r["word"] in low]
        key = (m.guild.id, m.author.id)
        self.bot.spam[key].append((discord.utils.utcnow(), low))
        recent = [t for t, _ in self.bot.spam[key] if (discord.utils.utcnow() - t).total_seconds() < 6]
        spammy = len(recent) >= 5
        if not hits and not spammy:
            return
        try: await m.delete()
        except discord.HTTPException: pass
        reason = f"Blocked word: {', '.join(hits)}" if hits else "Message spam"
        await ex("INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES (?,?,?,?,?)",
                 (m.guild.id, m.author.id, self.bot.user.id, f"[AUTOMOD] {reason}", discord.utils.utcnow().isoformat()))
        n = (await one("SELECT COUNT(*) c FROM warnings WHERE guild_id=? AND user_id=?", (m.guild.id, m.author.id)))["c"]
        note = f"**{n}/{s['automod_escalation']}**"
        if n >= s["automod_escalation"]:
            try:
                await m.author.timeout(discord.utils.utcnow() + timedelta(minutes=10), reason="Automod escalation")
                note += " — muted 10m"
            except discord.HTTPException: pass
        e = with_user(wrn(f"{m.author.mention} — {reason}\nWarnings: {note}", "🤖 Automod"), m.author)
        try: await m.channel.send(embed=e, delete_after=12)
        except discord.HTTPException: pass
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
        await setg(it.guild.id, antiraid_enabled=int(enabled), antiraid_threshold=threshold,
                   antiraid_window=window, antiraid_action=action, min_account_age=min_account_age)
        await it.response.send_message(embed=pnl("🛡️ Anti-Raid Configured",
            f"**Enabled:** {enabled}\n**Threshold:** {threshold} joins / {window}s\n**Action:** `{action}`\n**Min account age:** {min_account_age}d"))

    @app_commands.command(name="raidmode", description="Manually toggle lockdown")
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
            try: await m.send(embed=wrn(f"Your account is too new ({age}d) for **{m.guild.name}**.", "🚪 Join Rejected"))
            except discord.HTTPException: pass
            await m.kick(reason=f"Account age {age}d < {s['min_account_age']}d")
            return
        cache = self.bot.join_cache[m.guild.id]
        now = discord.utils.utcnow().timestamp()
        cache.append(now)
        recent = [t for t in cache if now - t <= s["antiraid_window"]]
        if len(recent) < s["antiraid_threshold"] or self.bot.raid_mode.get(m.guild.id, 0) > now - 30:
            return
        self.bot.raid_mode[m.guild.id] = now
        action = s["antiraid_action"]
        if action == "kick":
            try: await m.kick(reason="Raid protection")
            except discord.HTTPException: pass
        elif action == "lockdown":
            try: await m.guild.edit(verification_level=discord.VerificationLevel.highest)
            except discord.HTTPException: pass
        e = pnl("🚨 Raid Detected",
                f"**{len(recent)}** joins in **{s['antiraid_window']}s**\n**Action:** `{action}`\n**Trigger:** {m.mention} (`{m.id}`, {age}d old)", ERROR)
        await log_to(m.guild, "join_log", e)
        if m.guild.system_channel:
            try: await m.guild.system_channel.send(embed=e)
            except discord.HTTPException: pass


# ─────────────────────────── welcomer ───────────────────────────

class Welcomer(commands.Cog):
    def __init__(self, bot): self.bot = bot

    def render(self, text: str, m: discord.Member) -> str:
        return (text.replace("{user}", m.mention).replace("{name}", m.name)
                .replace("{server}", m.guild.name).replace("{count}", str(m.guild.member_count)))

    @app_commands.command(name="welcome", description="Configure the welcomer")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def welcome(self, it, channel: discord.TextChannel, title: str = "🎉 Welcome to {server}!",
                      message: str = "Hey {user}, welcome aboard!\nYou're member **#{count}**. Read the rules and verify to unlock the server.",
                      color: str = "5865F2", image: str | None = None, rules_url: str | None = None):
        try: col = int(color.replace("#", ""), 16)
        except ValueError: col = DEFAULT
        await setg(it.guild.id, welcome_channel=channel.id, welcome_title=title,
                   welcome_message=message, welcome_color=col, welcome_image=image)
        self.bot.add_view(WelcomeView(it.guild.id, rules_url))
        await it.response.send_message(embed=ok(f"Welcomer set to {channel.mention}.\nUse `/welcome_preview` to check it."))

    @app_commands.command(name="goodbye", description="Configure the goodbye message")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def goodbye(self, it, channel: discord.TextChannel, message: str = "👋 **{name}** left. We're down to **{count}** members."):
        await setg(it.guild.id, goodbye_channel=channel.id, goodbye_message=message)
        await it.response.send_message(embed=ok(f"Goodbye set to {channel.mention}."))

    @app_commands.command(name="welcome_preview", description="Preview the welcome embed")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def preview(self, it):
        s = await settings(it.guild.id)
        await it.response.send_message(embed=self.build(s, it.user), view=WelcomeView(it.guild.id))

    def build(self, s: dict, m) -> discord.Embed:
        e = E(self.render(s["welcome_message"] or "Welcome {user}!", s["welcome_color"] or DEFAULT),
              s["welcome_color"] or DEFAULT, self.render(s["welcome_title"] or "Welcome!", m))
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
            if ch:
                await ch.send(content=m.mention, embed=self.build(s, m), view=WelcomeView(m.guild.id))
        if s["join_log"]:
            ch = m.guild.get_channel(s["join_log"])
            if ch:
                await ch.send(embed=with_user(inf(f"{m.mention} joined.\n**Created:** <t:{int(m.created_at.timestamp())}:R>\n**ID:** `{m.id}`", "📥 Member Joined"), m))

    @commands.Cog.listener()
    async def on_member_remove(self, m: discord.Member):
        s = await settings(m.guild.id)
        if s["goodbye_channel"]:
            ch = m.guild.get_channel(s["goodbye_channel"])
            if ch:
                await ch.send(embed=with_user(pnl("👋 Member Left", self.render(s["goodbye_message"] or "**{name}** left.", m), ERROR), m))


# ─────────────────────────── verification (commands) ───────────────────────────

class Verification(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="verify", description="Set up verification")
    @app_commands.checks.has_permissions(administrator=True)
    async def setup(self, it, channel: discord.TextChannel, role: discord.Role,
                    unverified_role: discord.Role | None = None, log_channel: discord.TextChannel | None = None,
                    title: str = "🔐 Verification Required",
                    description: str = "Click the button below to verify through our website.\nYour IP is logged to keep alts and raiders out."):
        if not WEB_URL:
            return await it.response.send_message(embed=err("`WEB_URL` is not set on the host yet."))
        await setg(it.guild.id, verify_enabled=1, verify_role=role.id,
                   unverified_role=unverified_role.id if unverified_role else None,
                   verify_log=log_channel.id if log_channel else None)
        self.bot.add_view(VerifyView(it.guild.id))
        e = pnl(title, description, DEFAULT)
        e.add_field(name="Steps", value="1️⃣ Click **Verify**\n2️⃣ Authorize on the website\n3️⃣ You're in!")
        e.set_footer(text=it.guild.name, icon_url=it.guild.icon.url if it.guild.icon else None)
        await channel.send(embed=e, view=VerifyView(it.guild.id))
        await it.response.send_message(embed=ok(f"Panel posted in {channel.mention}. Role: {role.mention}"), ephemeral=True)

    @app_commands.command(name="verify_disable", description="Disable verification")
    @app_commands.checks.has_permissions(administrator=True)
    async def disable(self, it):
        await setg(it.guild.id, verify_enabled=0)
        await it.response.send_message(embed=ok("Verification disabled."))

    @app_commands.command(name="alts", description="Show detected alts for a member")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def alts(self, it, member: discord.Member):
        rows = await all_("SELECT * FROM alts WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        v = await one("SELECT * FROM verifications WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not v: return await it.response.send_message(embed=inf(f"{member.mention} hasn't verified."))
        body = "\n".join(f"• <@{r['matched_user_id']}> — {r['reason']}" for r in rows) or "None"
        await it.response.send_message(embed=with_user(pnl("👥 Alt Check",
            f"**IP:** `{v['ip']}`\n**Verified:** <t:{int(datetime.fromisoformat(v['verified_at']).timestamp())}:R>\n\n**Matches:**\n{body}"), member))

    @app_commands.command(name="whois", description="Show a member's verification record")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def whois(self, it, member: discord.Member):
        v = await one("SELECT * FROM verifications WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not v: return await it.response.send_message(embed=inf("No record."))
        await it.response.send_message(embed=with_user(pnl("🕵️ Verification Record",
            f"**IP:** `{v['ip']}`\n**User agent:** `{v['user_agent'][:80]}`\n**Verified:** <t:{int(datetime.fromisoformat(v['verified_at']).timestamp())}:F>"), member))


# ─────────────────────────── logs ───────────────────────────

class Logs(commands.Cog):
    def __init__(self, bot): self.bot = bot

    @app_commands.command(name="logs", description="Set a log channel")
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.describe(kind="all | mod | join | verify")
    async def logs(self, it, channel: discord.TextChannel, kind: str = "all"):
        m = {"all": "log_channel", "mod": "mod_log", "join": "join_log", "verify": "verify_log"}
        if kind not in m: return await it.response.send_message(embed=err("Use all, mod, join or verify."))
        await setg(it.guild.id, **{m[kind]: channel.id})
        await it.response.send_message(embed=ok(f"`{kind}` logs → {channel.mention}."))

    @commands.Cog.listener()
    async def on_message_delete(self, m: discord.Message):
        if not m.guild or m.author.bot: return
        await log_to(m.guild, "log_channel", with_user(pnl("🗑️ Message Deleted", f"**Channel:** {m.channel.mention}\n```{m.content[:900] or '—'}```", WARN), m.author))

    @commands.Cog.listener()
    async def on_message_edit(self, a: discord.Message, b: discord.Message):
        if not a.guild or a.author.bot or a.content == b.content: return
        await log_to(a.guild, "log_channel", with_user(pnl("✏️ Message Edited",
            f"**Channel:** {a.channel.mention}\n**Before:**\n```{a.content[:400]}```\n**After:**\n```{b.content[:400]}```", INFO), a.author))

    @commands.Cog.listener()
    async def on_member_update(self, a: discord.Member, b: discord.Member):
        if a.roles != b.roles:
            added = [r.mention for r in b.roles if r not in a.roles]
            removed = [r.mention for r in a.roles if r not in b.roles]
            if added or removed:
                await log_to(b.guild, "log_channel", with_user(pnl("🎭 Roles Updated",
                    f"**Added:** {', '.join(added) or '—'}\n**Removed:** {', '.join(removed) or '—'}", INFO), b))
        if a.nick != b.nick:
            await log_to(b.guild, "log_channel", with_user(pnl("📝 Nickname Changed", f"`{a.nick}` → `{b.nick}`", NEUTRAL), b))

    @commands.Cog.listener()
    async def on_voice_state_update(self, m, before, after):
        if before.channel == after.channel: return
        if before.channel is None:
            await log_to(m.guild, "log_channel", with_user(pnl("🔊 Joined Voice", after.channel.mention, SUCCESS), m))
        elif after.channel is None:
            await log_to(m.guild, "log_channel", with_user(pnl("🔈 Left Voice", before.channel.mention, ERROR), m))


# ─────────────────────────── staff ───────────────────────────

class Staff(commands.Cog):
    def __init__(self, bot): self.bot = bot

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
        n = (await one("SELECT COUNT(*) c FROM staff_ranks WHERE guild_id=?", (it.guild.id,)))["c"]
        await ex("INSERT INTO staff_ranks (guild_id,name,threshold,role_id,position) VALUES (?,?,?,?,?)",
                 (it.guild.id, name, points, role.id, n))
        await it.response.send_message(embed=ok(f"Rank **{name}** at `{points}` points → {role.mention}."))

    @app_commands.command(name="ranks", description="List promotion ranks")
    async def ranks(self, it):
        rows = await all_("SELECT * FROM staff_ranks WHERE guild_id=? ORDER BY threshold", (it.guild.id,))
        if not rows: return await it.response.send_message(embed=inf("No ranks configured."))
        await it.response.send_message(embed=pnl("📈 Promotion Ladder",
            "\n".join(f"`{r['threshold']}` → **{r['name']}** (<@&{r['role_id']}>)" for r in rows)))

    @app_commands.command(name="rank_remove", description="Remove a rank")
    @app_commands.checks.has_permissions(administrator=True)
    async def rank_remove(self, it, name: str):
        await ex("DELETE FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, name))
        await it.response.send_message(embed=ok(f"Removed rank `{name}`."))

    @app_commands.command(name="promote", description="Promote a staff member manually")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def promote(self, it, member: discord.Member, rank: str):
        r = await one("SELECT * FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, rank))
        if not r: return await it.response.send_message(embed=err("Unknown rank."))
        role = it.guild.get_role(r["role_id"])
        if not role: return await it.response.send_message(embed=err("Rank role no longer exists."))
        await member.add_roles(role, reason=f"Manual promotion by {it.user}")
        e = with_user(pnl("📈 Promotion", f"{member.mention} promoted to **{rank}** by {it.user.mention}.", SUCCESS), member)
        await it.response.send_message(embed=e); await log_to(it.guild, "log_channel", e)

    @app_commands.command(name="demote", description="Remove a rank role")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def demote(self, it, member: discord.Member, rank: str):
        r = await one("SELECT * FROM staff_ranks WHERE guild_id=? AND name=?", (it.guild.id, rank))
        if not r: return await it.response.send_message(embed=err("Unknown rank."))
        role = it.guild.get_role(r["role_id"])
        if role: await member.remove_roles(role, reason=f"Demoted by {it.user}")
        await it.response.send_message(embed=ok(f"{member.mention} demoted from **{rank}**."))

    @app_commands.command(name="staffstats", description="View a staff member's record")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def staffstats(self, it, member: discord.Member):
        r = await one("SELECT * FROM staff WHERE guild_id=? AND user_id=?", (it.guild.id, member.id))
        if not r: return await it.response.send_message(embed=inf("No staff record."))
        h, m = r["total_seconds"] // 3600, (r["total_seconds"] % 3600) // 60
        await it.response.send_message(embed=with_user(pnl("📊 Staff Record",
            f"**Points:** `{r['points']}`\n**Days present:** `{r['days_present']}`\n**Total time:** `{h}h {m}m`\n"
            f"**On duty:** {'🟢' if r['on_duty'] else '🔴'}\n**Last sign-in:** {r['last_signin'] or '—'}"), member))

    @app_commands.command(name="stafftop", description="Staff activity leaderboard")
    async def stafftop(self, it):
        rows = await all_("SELECT * FROM staff WHERE guild_id=? ORDER BY points DESC LIMIT 10", (it.guild.id,))
        if not rows: return await it.response.send_message(embed=inf("No data yet."))
        medals = ["🥇", "🥈", "🥉"] + ["▪️"] * 7
        await it.response.send_message(embed=pnl("🏆 Staff Leaderboard",
            "\n".join(f"{medals[i]} <@{r['user_id']}> — `{r['points']}` pts · `{r['days_present']}` days" for i, r in enumerate(rows))))


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
            f"{t(s['verify_enabled'])} `/verify` — verification panel",
            f"{t(s['staff_channel'])} `/staff` — duty panel",
            f"{t(await one('SELECT 1 FROM staff_ranks WHERE guild_id=?', (it.guild.id,)))} `/rank_add` — promotion ladder",
            f"{t(s['antiraid_enabled'])} `/antiraid` — raid protection",
        ])))

    @app_commands.command(name="help", description="List all commands")
    async def help_(self, it):
        e = pnl("📖 Command Reference", "Every command replies with an embed.", DEFAULT)
        e.add_field(name="🛡️ Moderation", value="`ban` `unban` `kick` `softban` `timeout` `untimeout` `warn` `warnings` `delwarn` `purge` `slowmode` `lock` `unlock` `nick` `addrole` `removerole`")
        e.add_field(name="🤖 Automod", value="`automod` `blockword` `unblockword` `blockedwords`")
        e.add_field(name="🚨 Anti-Raid", value="`antiraid` `raidmode`")
        e.add_field(name="👋 Welcomer", value="`welcome` `goodbye` `welcome_preview`")
        e.add_field(name="🔐 Verification", value="`verify` `verify_disable` `alts` `whois`")
        e.add_field(name="📋 Logs", value="`logs`")
        e.add_field(name="🛡️ Staff", value="`staff` `rank_add` `ranks` `rank_remove` `promote` `demote` `staffstats` `stafftop`")
        await it.response.send_message(embed=e, ephemeral=True)


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
    if bot.loop is None:
        raise RuntimeError("Bot loop not ready")
    return asyncio.run_coroutine_threadsafe(coro, bot.loop).result(timeout)


@app.get("/")
def index():
    return page("Verification Gateway", "Add the bot and run /verify in your server to get started.", "🛡️")


@app.get("/health")
def health():
    return {"ok": True, "guilds": len(bot.guilds), "ready": bot.is_ready()}


@app.get("/verify")
def verify():
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
        return page("Authorization Failed", "Discord rejected the code. Try again.", "❌"), 400

    me = requests.get(f"{API}/users/@me", headers={"Authorization": f"Bearer {tok.json()['access_token']}"}, timeout=15)
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

    now = datetime.now(timezone.utc).isoformat()
    await ex("INSERT INTO verifications (guild_id,user_id,ip,user_agent,verified_at) VALUES (?,?,?,?,?)",
             (gid, user["id"], ip, ua, now))

    alts = await all_("SELECT user_id FROM verifications WHERE guild_id=? AND ip=? AND user_id!=?", (gid, ip, user["id"]))
    for a in alts:
        await ex("INSERT INTO alts (guild_id,user_id,matched_user_id,reason,created_at) VALUES (?,?,?,?,?)",
                 (gid, user["id"], a["user_id"], f"same IP ({ip})", now))

    try:
        member = guild.get_member(user["id"]) or await guild.fetch_member(user["id"])
        if s["verify_role"] and (role := guild.get_role(s["verify_role"])):
            await member.add_roles(role, reason="Verification")
        if s["unverified_role"] and (role := guild.get_role(s["unverified_role"])) and role in member.roles:
            await member.remove_roles(role, reason="Verification")
    except discord.HTTPException:
        pass

    if s["verify_log"] and (ch := guild.get_channel(s["verify_log"])):
        created = datetime.fromtimestamp(((int(user["id"]) >> 22) + 1420070400000) / 1000, tz=timezone.utc)
        age = (datetime.now(timezone.utc) - created).days
        avatar = (f"https://cdn.discordapp.com/avatars/{user['id']}/{user['avatar']}.png"
                  if user.get("avatar") else "https://cdn.discordapp.com/embed/avatars/0.png")
        e = pnl("🔐 Member Verified", f"<@{user['id']}> verified.", SUCCESS)
        e.add_field(name="IP", value=f"`{ip}`", inline=True)
        e.add_field(name="Account age", value=f"`{age}d`", inline=True)
        if alts:
            e.add_field(name="Possible alts", value="\n".join(f"<@{a['user_id']}>" for a in alts[:10]), inline=False)
        e.set_thumbnail(url=avatar)
        await ch.send(embed=e)
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


if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True), daemon=True).start()
    log.info("Web server bound to :%s", PORT)
    threading.Thread(target=keep_alive, daemon=True).start()
    bot.run(TOKEN, log_handler=None)