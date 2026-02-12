import json
import subprocess
import os
import sys
from datetime import datetime

CONFIG_FILE = "config.json"
REPO = "leo-cloudarbitration/functions"
LOCK_FILE = "/tmp/scheduler.lock"
LOG_FILE = "/home/ubuntu/scheduler/scheduler.log"

def log_event(event):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")

def acquire_lock():
    if os.path.exists(LOCK_FILE):
        log_event({
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "warning",
            "message": "Scheduler já está rodando. Abortando nova execução."
        })
        sys.exit(0)
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def should_run_hourly(config, current_minute):
    return config.get("minute") == current_minute

def should_run_daily(config, current_hour, current_minute):
    time_str = config.get("time")
    if not time_str:
        return False
    hour, minute = map(int, time_str.split(":"))
    return hour == current_hour and minute == current_minute

def trigger_workflow(workflow):
    start_time = datetime.utcnow().isoformat() + "Z"

    result = subprocess.run(
        ["gh", "workflow", "run", workflow, "--repo", REPO],
        capture_output=True,
        text=True
    )

    event = {
        "timestamp": start_time,
        "workflow": workflow,
        "exit_code": result.returncode,
        "status": "success" if result.returncode == 0 else "error",
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip()
    }

    log_event(event)

def main():
    print("Scheduler executando agora...")
    acquire_lock()

    try:
        now = datetime.utcnow()
        current_hour = now.hour
        current_minute = now.minute

        with open(CONFIG_FILE) as f:
            config = json.load(f)

        for workflow, settings in config.items():

            if workflow.startswith("_"):
                continue

            run = False
            job_type = settings.get("type")

            if job_type == "hourly":
                run = should_run_hourly(settings, current_minute)

            elif job_type == "daily":
                run = should_run_daily(settings, current_hour, current_minute)

            if run:
                trigger_workflow(workflow)

    finally:
        release_lock()

if __name__ == "__main__":
    main()
