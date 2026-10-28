#!/bin/bash
# Weekly Peer Validation report + email. Called by cron every Monday morning.
#   crontab -e   ->   30 7 * * 1 /home/analytics/pyspark/jobs/peer_validation/run_peer_validation.sh
# Keep this file readable only by the job owner:  chmod 700 run_peer_validation.sh

# ---- paths (change to where the script lives on the server) ----
JOB_DIR="/home/analytics/pyspark/jobs/stage-2/"
PYTHON="/usr/bin/python3"                       # same python you use to run it by hand (`which python3`)
LOG_DIR="/home/analytics/pyspark/logs/peer_validation"

# ---- mail settings ----
export PV_SMTP_HOST="smtp.gmail.com"
export PV_SMTP_PORT="587"
export PV_SMTP_USER="kbalert@karmayogibharat.net"
export PV_SMTP_PASSWORD="sixiyzfdgadgkyrz"     # use an app password / service account, not a personal password
export PV_MAIL_FROM="mission.karmayogi@gov.in"
export PV_MAIL_TO="deepak.goswami@tarento.com,reshma.prasad@tarento.com,sonu.gowda@tarento.com,manish.gupta@tarento.com,arjun.singh@tarento.com,chinmay.chaitanya@karmayogi.in,cpo.karmayogi@gov.in,aaggarwal5@deloitte.com,coo.karmayogi@gov.in"
export PV_MAIL_CC="christopher.fernandes@tarento.com,varsha.mahuli@tarento.com"
export PV_MAIL_SUBJECT="Peer Validation Weekly Report: {from} - {to}"   # {from}/{to} =
export PV_ALERT_TO="varsha.mahuli@tarento.com,christopher.fernandes@tarento.com"
# ---- cron runs with a bare environment: set what Spark / Java need ----
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/"
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH"

mkdir -p "$LOG_DIR"
cd "$JOB_DIR" || exit 1
nice -n 10 "$PYTHON" peerValidationWeeklyReq.py "$@" >> "$LOG_DIR/run_$(date +%F).log" 2>&1
