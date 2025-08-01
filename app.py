import os
import logging
from datetime import date, datetime, timezone
from collections import defaultdict
from telebot import types   # add this import at the top
import time                # needed for the polling loop
import certifi
import requests
import schedule
import telebot
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState
from dotenv import load_dotenv

os.environ["SSL_CERT_FILE"] = certifi.where()

load_dotenv()          # loads from .env if you keep secrets there
BOT_TOKEN   = os.environ["BOT_TOKEN"]
CHAT_ID     = int(os.environ["CHAT_ID"])       # e.g. 123456789

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)

# ------------------------------------------------------------------
# Telegram bot handlers (stay responsive while polling)
# ------------------------------------------------------------------
@bot.message_handler(commands=["start", "hello"])
def send_welcome(message):
    bot.reply_to(message, "Howdy, how are you doing?")

@bot.message_handler(func=lambda msg: True)
def echo_all(message):
    bot.reply_to(message, message.text)

# ------------------------------------------------------------------
# Databricks reporting
# ------------------------------------------------------------------
def databricks_job_notification():
    """
    Collect all failed runs from today for the creator,
    send a summary + inline keyboard asking whether to repair.
    """
    today = date.today()
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_SERVER"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    failed_runs_today = []

    for job in w.jobs.list():
        if job.creator_user_name != os.environ['EMAIL']:
            continue
        for run in w.jobs.list_runs(job_id=job.job_id, expand_tasks=False):
            if (
                run.state.result_state is RunResultState.FAILED
                and run.end_time
                and datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc).date() == today
            ):
                failed_runs_today.append(
                    {
                        "job": job,
                        "run": run,
                    }
                )

    # Build message
    if not failed_runs_today:
        bot.send_message(CHAT_ID, "🎉 No failures today!")
        return

    lines = [f"❌ Found {len(failed_runs_today)} failed run(s) today:"]
    for item in failed_runs_today:
        lines.append(
            f" • {item['job'].settings.name}  (run_id={item['run'].run_id})"
        )
    lines.append("")
    lines.append("Repair (rerun all failed tasks) ?")

    # Inline keyboard
    kb = types.InlineKeyboardMarkup()
    yes_btn = types.InlineKeyboardButton(
        text="✅ Yes – repair", callback_data="repair_yes"
    )
    no_btn = types.InlineKeyboardButton(text="❌ No", callback_data="repair_no")
    kb.add(yes_btn, no_btn)

    bot.send_message(CHAT_ID, "\n".join(lines), reply_markup=kb)

# ------------------------------------------------------------------
# Callback handler for the inline buttons
# ------------------------------------------------------------------
@bot.callback_query_handler(func=lambda call: call.data.startswith("repair_"))
def handle_repair_choice(call):
    """
    React to the Yes/No button press.
    """
    if call.data == "repair_yes":
        # Re-collect the same list (or cache it if you prefer)
        today = date.today()
        w = WorkspaceClient(
            host=os.environ["DATABRICKS_SERVER"],
            token=os.environ["DATABRICKS_TOKEN"],
        )
        repaired = []
        for job in w.jobs.list():
            if job.creator_user_name != "anurag.pal@zeptonow.com":
                continue
            for run in w.jobs.list_runs(job_id=job.job_id, expand_tasks=False):
                if (
                    run.state.result_state is RunResultState.FAILED
                    and run.end_time
                    and datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc).date()
                    == today
                ):
                    w.jobs.repair_run(run.run_id, rerun_all_failed_tasks=True)
                    repaired.append(f"{job.settings.name} (run_id={run.run_id})")
        bot.answer_callback_query(call.id, "Repair triggered ✅")
        bot.send_message(CHAT_ID, "Started repair for:\n" + "\n".join(repaired))
    else:
        bot.answer_callback_query(call.id, "Skipped ❌")


# ------------------------------------------------------------------
# Schedule
# ------------------------------------------------------------------
for t in ("09:00", "12:00", "15:00", "18:14"):
    schedule.every().day.at(t).do(databricks_job_notification)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    databricks_job_notification()          # run once on start-up
    while True:
        schedule.run_pending()
        bot.process_pending_updates()
        time.sleep(1)
