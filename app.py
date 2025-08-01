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
CHAT_ID     = int(os.environ["CHAT_ID"])       

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)

# ------------------------------------------------------------------
# Telegram bot handlers (stay responsive while polling)
# ------------------------------------------------------------------
@bot.message_handler(commands=["start", "hello"])
def send_welcome(message):
    bot.reply_to(message, "Howdy, how are you doing?")


# ------------------------------------------------------------------
# Databricks reporting
# ------------------------------------------------------------------
def databricks_job_notification():
    """
    Collect all failed runs from today for the creator,
    send a summary + inline keyboard to pick *which* job to repair.
    Only one job will be repaired at a time.
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
                and datetime.fromtimestamp(run.end_time / 1000, tz=timezone.utc).date()
                == today
            ):
                failed_runs_today.append(
                    {
                        "job": job,
                        "run": run,
                    }
                )

    if not failed_runs_today:
        bot.send_message(CHAT_ID, "🎉 No failures today!")
        return

    # Build message
    lines = [f"❌ Found {len(failed_runs_today)} failed run(s) today. Pick ONE to repair:"]
    bot.send_message(CHAT_ID, "\n".join(lines))

    # One inline keyboard per failed run
    for item in failed_runs_today:
        kb = types.InlineKeyboardMarkup()
        repair_btn = types.InlineKeyboardButton(
            text=f"Repair {item['job'].settings.name}",
            callback_data=f"repair_{item['run'].run_id}"
        )
        kb.add(repair_btn)
        bot.send_message(
            CHAT_ID,
            f"{item['job'].settings.name}  (run_id={item['run'].run_id})",
            reply_markup=kb
        )


# ------------------------------------------------------------------
# Callback handler for the inline buttons
# ------------------------------------------------------------------
@bot.callback_query_handler(func=lambda call: call.data.startswith("repair_"))
def handle_repair_choice(call):
    run_id = int(call.data.split("_", 1)[1])
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_SERVER"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    # get job name from the run
    run_info = w.jobs.get_run(run_id)
    #job_name = run_info.job.settings.name

    w.jobs.repair_run(run_id, rerun_all_failed_tasks=True)
    bot.answer_callback_query(call.id, "Repair triggered ✅")
    bot.send_message(
        CHAT_ID,
        f"Started repair for job  (run_id={run_id})",
        parse_mode="Markdown"
    )


# ------------------------------------------------------------------
# Schedule
# ------------------------------------------------------------------
for t in ("09:00", "12:00", "15:00", "18:42"):
    schedule.every().day.at(t).do(databricks_job_notification)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    databricks_job_notification()          # run once on start-up
    while True:
        schedule.run_pending()
        bot.polling()
        time.sleep(1)
