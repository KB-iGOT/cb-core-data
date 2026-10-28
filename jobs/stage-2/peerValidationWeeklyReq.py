"""
Peer Validation - weekly report (run every Monday).

Each run writes two folders under OUT_BASE/<run_date>/ :

  cumulative/                      everything from launch till date
      summary_table.csv            course-wise (Phase 1: 3 AI courses, Phase 2: 68 courses):
                                   Eligible, Submissions, Total Peers, Approved, Rejected, No Action
                                   (+ Phase 1 Total, Phase 2 Total, overall Total rows)
      Reviewers.csv                Netcore file
      Survey users.csv             Netcore file
      user_responses.csv           form_id, user_id, question, response
      peer_responses.csv           form_id, submitter, peer_id, APPROVED / REJECTED / NA

  week_<from>_to_<to>/             only activity inside the week: summary_table, Reviewers, Survey users

PHASE_FLAG: Phase 1 / Phase 2 by course (lists in CONFIG); a user with tasks in both = Both.

Netcore columns (DO NOT CHANGE):
  USERID, PARENT_MINISTRY, PARENT_DEPARTMENT, ORGANISATION, PHASE_FLAG, COMPLETION_STATUS

CURRENT OUTPUT (see GENERATE_* flags in CONFIG to switch the rest back on):
  week_<from>_to_<to>/user_responses.csv   answers submitted in the week
  week_<from>_to_<to>/peer_responses.csv   peer assignments on the week's submissions + reviews done in the week
  week_<from>_to_<to>/Reviewers.csv        Netcore: reviewers with activity in the week (status = all their reviews)
  week_<from>_to_<to>/Survey users.csv     Netcore: survey users with activity in the week (status = all their surveys)
  Mail attachments: peer_validation_files_<week>.zip (responses) + netcore_files_<week>.zip (Netcore files)
  Email: "Dear Sir/Ma'am" note, weekly tiles + Course-wise Adoption table (Phase 1 + Phase 2),
         with the two CSVs attached (as one zip). On failure: NO mail to recipients; the job exits
         with an error in the log (optional alert to PV_ALERT_TO only).
SMTP settings / recipients come from environment variables (see run_peer_validation.sh).
Use --no-email to build files without sending.

Week window:
  Every Monday:          python3 peerValidationReport.py                 -> previous Monday .. Sunday
  First run (19-10-26):  python3 peerValidationReport.py --week-from 2026-09-01   -> 1 Sep .. 18 Oct
  Custom:                python3 peerValidationReport.py --week-from 2026-10-19 --week-to 2026-10-25
"""
import findspark
findspark.init()

import argparse
import glob
import html
import os
import shutil
import smtplib
import sys
import time
import traceback
from email.message import EmailMessage
from datetime import date, datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

sys.path.append(str(Path(__file__).resolve().parents[2]))
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.utils import utils
from jobs.default_config import create_config
from jobs.config import get_environment_config

# ============================================================================
# CONFIG
# ============================================================================
PHASE_1_COURSES = {
    "do_114371136825573376161": "Introduction to Artificial Intelligence",
    "do_1145242891094835201284": "Artificial Intelligence for Karmayogis",
    "do_114371167997878272169": "Core Areas of Artificial Intelligence",
}
PHASE_2_COURSES = {   # course_id: course name   (form id in comment; '-' = no form yet)
    "do_1143438322170675201188": 'Fair Compensation and Land Acquisition Act 2013',  # 1789843158441
    "do_1143245150366023681143": 'Climate Change Learning Through Actions',  # 1789843650929
    "do_11426698022091980812": 'Using Behaviour Science for Climate Adaptation and Mitigation',  # 1789843852771
    "do_11397616050638848015": 'Navigating Climate Challenges - A Guide for Civil Servants',  # 1789844543598
    "do_114125692631474176140": 'Navigating Climate Challenges - A Guide for Civil Servants (Hindi)',  # 1789844714369
    "do_11436702315415142411794": 'An Introduction to Vermicomposting',  # 1789844881664
    "do_11422319743288115211350": 'Overview of Pradhan Mantri Fasal Bima Yojana',  # 1789845048196
    "do_1144186912498728961425": 'Urban Flooding Challenges and Solutions',  # 1789845209598
    "do_1143927216462970881487": 'Solid Waste Management under Swachh Bharat Mission Urban 2.0',  # 1789845369945
    "do_11434937918826905611033": 'Urban Transport Management',  # 1789845563734
    "do_114318843800133632145": 'Artificial Intelligence in Finance',  # 1789845918661
    "do_1141999407663022081143": 'Personal Finance',  # 1789846204593
    "do_1138877270341550081138": 'UNDERSTANDING REVENUE RECOGNITION PRINCIPLE',  # 1789846381644
    "do_1138589039041986561110": 'Cancellation and Revocation of GST Registration',  # 1789871389221
    "do_1142990493428203521581": 'प्रधानमंत्री मातृ वंदना योजना 2.0',  # 1789846837917
    "do_1144510630223134721721": 'स्वस्थ शुरुआत: बाल पोषण और देखभाल',  # 1789847167478
    "do_114353453953073152153": 'प्रारंभिक बाल्यावस्था उत्प्रेरण',  # 1789847434229
    "do_114430676366409728168": 'नगरेत्तर भूमि सर्वे एवं डिजिटल अभिलेख अद्यतन परियोजना',  # 1789847635570
    "do_1144603104511836161117": 'Introduction to Remote Sensing and Satellite Data',  # 1789847813610
    "do_1145200741172346881926": 'Geospatial Data Analysis with LiDAR and GNSS',  # -
    "do_1143542017034076161264": 'विद्युत झटकों से सुरक्षा और जीवन रक्षक तकनीकों पर प्रस्तुति',  # 1789848259315
    "do_1144468775837532161544": '33/11 के.वी. सब-स्टेशन का संचालन एवं निरीक्षण और रखरखाव',  # 1789848468742
    "do_1134122081611939841114": 'Snippet Course - Transmission Line Towers',  # 1789849564036
    "do_11436627954176819211709": 'Design and Laying of Medium Voltage Electrical Cables',  # 1789849851406
    "do_1142544063186370561163": 'Kavach - Automatic Train Protection (ATP) System',  # 1789850033930
    "do_114095780873625600147": 'High-Speed Rail Development: Context and Impact',  # 1789850230428
    "do_1142345499997716481282": 'PM GatiShakti National Master Plan and Indian Railways',  # 1789871797623
    "do_113854667716444160153": "'Namami Gange : A Sustainable Model for River Rejuvenation'",  # 1789850585330
    "do_1138431041701724161194": 'ATAL BHUJAL YOJANA',  # 1789850746133
    "do_1139846519534469121107": 'Command Area Development and Participatory Irrigation Management',  # 1789850903289
    "do_1142651221437808641135": 'सड़क दुर्घटनाएँ: कारण एवं निवारण 2024-2025',  # 1789851072858
    "do_1144611663990947841165": 'Corrosion Control of Reinforced Concrete Structures',  # 1789851274040
    "do_1145767790455439361438": 'Hill Road Safety and Design Fundamentals',  # 1789851450783
    "do_114083886873051136118": 'DHFC - Module 1 - Introduction to Healthcare Systems in India',  # 1789851634297
    "do_11422470238834688011407": 'DHFC - Module 2 - What is Digital Health',  # 1789851649846
    "do_1142106154329948161793": 'DHFC - Module 3 - Introduction to HMIS and EMR',  # 1789851665401
    "do_114180230057369600162": 'DHFC - Module 4 - Introduction to Imaging and Departmental Systems',  # 1789851680962
    "do_114085336305582080146": 'DHFC - Module 5 - Healthcare Data Interoperability',  # 1789851696520
    "do_114085343053619200148": 'DHFC - Module 6 - Healthcare Data Privacy and Security',  # 1789851712066
    "do_114085351572168704156": 'DHFC - Module 7 - Introduction to Ayushman Bharat Digital Mission',  # 1789851727614
    "do_114085356224651264150": 'DHFC - Module 8 - Personal Health Empowerment in Digital Age',  # 1789851743242
    "do_114085360967229440152": 'DHFC - Module 9 - Telemedicine',  # 1789851758802
    "do_1141049552951705601228": 'DHFC - Module 10 - Health Data Analytics and Indicators',  # 1789851774362
    "do_1141055241509355521220": 'DHFC - Module 11 - Application of AI and ML in Healthcare',  # 1789851789915
    "do_1141056668043100161228": 'DHFC - Module 12 - Benefits of Digital Health in Public Health',  # 1789851805480
    "do_1141057105564221441242": 'DHFC - Module 13 - Opportunities in Digital Health Sector',  # 1789851821051
    "do_1143089865482649601691": 'Fundamentals of Public Policy',  # 1789852014177
    "do_113956414673690624149": 'Public Governance Models',  # 1789852175541
    "do_1139301654992896001153": 'Public Policy Writing',  # 1789852355199
    "do_1144179593210757121299": 'लोक नीति लेखन',  # 1789852529058
    "do_1143396131755540481556": 'Understanding the National Education Policy 2020',  # 1789852705944
    "do_114362631072153600120": 'राष्ट्रीय शिक्षा नीति 2020',  # 1789871955406
    "do_1144504750203944961707": 'समावेशी शिक्षा',  # 1789872103095
    "do_11384823508410368016": "Basic information of Citizens' Charter",  # 1789853262826
    "do_1142005081508003841219": 'Post Office Act 2023',  # 1789853414653
    "do_1142114066177310721226": 'EKYC Implementation and Aadhaar Data Handling',  # -
    "do_11391600147094732814": 'GFR 2017 - Procurement',  # -
    "do_1134970386480578561102": 'Public Procurement Framework of GOI',  # -
    "do_113695326810488832115": 'Public Procurement of Goods',  # -
    "do_1144379268466114561226": 'Police as First Responders in Disaster Management',  # 1789854212623
    "do_113943050241949696168": 'Disaster Management Plan for natural and man-made disasters M-1 Hindi',  # 1789854228166
    "do_1141063963791933441311": 'Drought-Disaster Response Training',  # 1789854243715
    "do_1139954349659095041123": 'Introduction to Bharatiya Sakshya Adhiniyam, 2023',  # 1789854424887
    "do_1139954106661601281113": 'Introduction To Bharatiya Nyaya Sanhita, 2023',  # 1789854440444
    "do_1139954244450385921110": 'Introduction to Bharatiya Nagarik Suraksha Sanhita, 2023',  # 1789854455999
    "do_1140492679108526081344": 'Overview of Bharatiya Sakshya Adhiniyam, 2023 for Field Practitioners',  # 1789854664621
    "do_1140492429858242561335": 'Overview of Bharatiya Nyaya Sanhita, 2023 for Field Practitioners',  # 1789854680183
    "do_1140492534506618881342": 'Overview of BNSS, 2023 for Field Practitioners',  # 1789854695750
}
COURSE_PHASE = {**{c: "Phase 1" for c in PHASE_1_COURSES}, **{c: "Phase 2" for c in PHASE_2_COURSES}}
COURSE_IDS = list(COURSE_PHASE)
OUT_BASE = "/home/analytics/pyspark/reports/peer_validation"

NETCORE_COLS = ["USERID", "PARENT_MINISTRY", "PARENT_DEPARTMENT", "ORGANISATION",
                "PHASE_FLAG", "COMPLETION_STATUS"]           # exact Netcore column names - DO NOT CHANGE
DONE_STATUSES = ["APPROVED", "REJECTED"]

# ---- Email (secrets come from environment variables, see run_peer_validation.sh) ----
SMTP_HOST     = os.environ.get("PV_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("PV_SMTP_PORT", "587"))          # 587 = STARTTLS, 465 = SSL
SMTP_USER     = os.environ.get("PV_SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("PV_SMTP_PASSWORD", "")
MAIL_FROM     = os.environ.get("PV_MAIL_FROM", SMTP_USER)
MAIL_TO       = [x.strip() for x in os.environ.get("PV_MAIL_TO", "").split(",") if x.strip()]
MAIL_CC       = [x.strip() for x in os.environ.get("PV_MAIL_CC", "").split(",") if x.strip()]
# failure alerts go ONLY here (never to MAIL_TO / MAIL_CC). Empty = no failure mail, just the log.
ALERT_TO      = [x.strip() for x in os.environ.get("PV_ALERT_TO", "").split(",") if x.strip()]
# {from} / {to} are replaced with the week dates, e.g. "19 Oct" / "25 Oct 2026"
MAIL_SUBJECT  = os.environ.get("PV_MAIL_SUBJECT", "Peer Validation Weekly Report: {from} - {to}")
MAX_ATTACH_MB = 18        # max zip size to attach (Gmail limit is 25 MB after encoding, ~18 MB of file)

# ---- What to generate. Currently only the WEEKLY user/peer responses + the mail table.
# Everything else is still in the code; switch a flag to True to bring it back.
GENERATE_CUMULATIVE  = False   # cumulative/ folder (all files from launch till date)
GENERATE_SUMMARY_CSV = False   # summary_table.csv (the same table that goes in the mail)
GENERATE_NETCORE     = True    # Reviewers.csv / Survey users.csv (weekly; attached as a separate zip)
# ============================================================================


def write_csv(df, out_dir, name, quote_all=False):
    tmp = os.path.join(out_dir, f"_{name}_tmp")
    (df.coalesce(1).write.mode("overwrite")
     .option("header", True).option("quote", '"').option("escape", '"').option("quoteAll", quote_all)
     .csv(tmp))
    shutil.move(glob.glob(os.path.join(tmp, "part-*.csv"))[0], os.path.join(out_dir, f"{name}.csv"))
    shutil.rmtree(tmp)
    print(f"  wrote {os.path.basename(out_dir)}/{name}.csv", flush=True)


def in_window(c, win):
    """win = None (cumulative) or (from_date, to_date) inclusive."""
    if win is None:
        return F.lit(True)
    return col(c).between(F.lit(win[0].isoformat()).cast("date"), F.lit(win[1].isoformat()).cast("date"))


class PeerValidationReport:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def read_es(self, index, fields):
        query = {"bool": {"must": [{"match": {"contextType": "peerValidationSurvey"}}]}}
        return utils.read_elasticsearch_data_scroll(
            self.spark, self.config.sparkIGotElasticsearchConnectionHost,
            self.config.sparkElasticsearchConnectionPort, index, fields=fields, query=query)

    # ------------------------------------------------------------------ load
    def load(self):
        forms = (self.read_es("fs-forms-alias-v2",
                              ["formId", "contextType", "status", "additionalProperties", "createdFor", "createdDate"])
                 .withColumn("createdDate", F.from_unixtime(col("createdDate") / 1000).cast("timestamp"))
                 .select(col("formId").alias("form_id"), "createdFor", "createdDate",
                         col("additionalProperties.identifier").alias("course_id"),
                         col("additionalProperties.completionLookBack").cast("int").alias("max_trigger_days"),
                         col("additionalProperties.isSpvCreated").alias("is_spv_created"))
                 .filter(col("course_id").isin(COURSE_IDS))
                 .withColumn("first_trigger_start", F.date_sub(col("createdDate"), col("max_trigger_days")))
                 .withColumn("first_trigger_end", F.current_date()))
        phase_map = self.spark.createDataFrame(list(COURSE_PHASE.items()), ["course_id", "phase"])
        self.forms = forms.join(F.broadcast(phase_map), "course_id").cache()

        # one row per submission (joined to one-row-per-form, so no duplication)
        self.subs = (self.read_es("fs-forms-data-alias-v2",
                                  ["formId", "contextType", "contextId", "responses", "peerReviews",
                                   "submittedBy", "submittedDate"])
                     .join(self.forms.select(col("form_id").alias("formId")), "formId")
                     .withColumn("submitted_on", F.to_date(F.from_unixtime(col("submittedDate") / 1000)))
                     .withColumn("submission_id", F.md5(F.concat_ws("|", "formId", "submittedBy", "submittedDate")))
                     .cache())

        # one row per (submission, assigned peer)
        self.peers = (self.subs.withColumn("pr", F.explode("peerReviews"))
                      .select("submission_id", col("formId").alias("form_id"), col("contextId").alias("course_id"),
                              col("submittedBy").alias("submitter_id"), "submitted_on",
                              col("pr.peerId").alias("peer_id"), col("pr.status").alias("peer_status"),
                              F.to_date(F.from_unixtime(col("pr.reviewedAt") / 1000)).alias("reviewed_on"))
                      .filter(col("peer_id").isNotNull())
                      .withColumn("review_done", col("peer_status").isin(DONE_STATUSES).cast("int"))
                      .cache())

        # one row per (submission, answered question)
        self.responses = (self.subs.withColumn("r", F.explode("responses"))
                          .select("submission_id", col("formId").alias("form_id"),
                                  col("contextId").alias("course_id"),
                                  col("submittedBy").alias("user_id"), "submitted_on",
                                  col("r.question").cast("string").alias("question"),
                                  col("r.answer").cast("string").alias("answer"))
                          .filter(col("answer").isNotNull())
                          .cache())

        self.users = (self.spark.read.parquet(ParquetFileConstants.USER_ORG_COMPUTED_FILE)
                      .select(col("userID").alias("user_id"), "userOrgID", "fullName", "userPrimaryEmail",
                              "userMobile", "ministry_name", "dept_name", "userOrgName")
                      .dropDuplicates(["user_id"]).cache())

        names = {**PHASE_1_COURSES, **PHASE_2_COURSES}
        self.courses = self.spark.createDataFrame(
            [(c, names[c], p) for c, p in COURSE_PHASE.items()], ["course_id", "course_name", "phase"])

        # eligible = received the survey: (user, form) + the date they became eligible (course completion)
        forms_by_org = (self.forms.withColumn("org", F.explode_outer("createdFor"))
                        .withColumn("form_org_id", col("org.orgId")).drop("org", "createdFor"))
        self.eligible = (self.spark.read.parquet(ParquetFileConstants.ENROLMENT_COMPUTED_PARQUET_FILE)
                         .select(col("userID").alias("user_id"), col("courseID").alias("course_id"),
                                 "firstCompletedOn", "certificateID", "dbCompletionStatus")
                         .filter(col("certificateID").isNotNull() & (col("certificateID") != "")
                                 & (col("dbCompletionStatus") == "2") & col("course_id").isin(COURSE_IDS))
                         .withColumn("completed_on", F.to_date(F.date_format(
            "firstCompletedOn", ParquetFileConstants.DATE_TIME_FORMAT)))
                         .join(self.users.select("user_id", "userOrgID"), "user_id", "left")
                         .join(forms_by_org, "course_id")
                         .filter(((col("is_spv_created") == True)
                                  | ((col("is_spv_created") == False) & (col("userOrgID") == col("form_org_id"))))
                                 & col("completed_on").between(col("first_trigger_start"), col("first_trigger_end")))
                         .groupBy("user_id", "form_id", "course_id")
                         .agg(F.min("completed_on").alias("completed_on"))
                         .cache())

    # --------------------------------------------------------- summary table
    def summary_table(self, win):
        """Cumulative (win=None) or week:
             Eligible     = became eligible (completed the course) in the period
             Submissions  = submitted in the period
             Total Peers  = peer assignments on submissions made in the period
             Approved / Rejected = reviews actioned in the period
             No Action    = peer assignments (on the period's submissions) still not actioned"""
        elig = (self.eligible.filter(in_window("completed_on", win))
                .groupBy("course_id").agg(F.countDistinct("user_id").alias("Eligible")))
        subs = (self.subs.filter(in_window("submitted_on", win))
                .groupBy(col("contextId").alias("course_id"))
                .agg(F.countDistinct("submission_id").alias("Submissions")))
        assigned = (self.peers.filter(in_window("submitted_on", win))
                    .groupBy("course_id")
                    .agg(F.count("*").alias("Total Peers"),
                         F.sum(1 - col("review_done")).alias("No Action")))
        actioned = (self.peers.filter(in_window("reviewed_on", win) if win else F.lit(True))
                    .groupBy("course_id")
                    .agg(F.sum((col("peer_status") == "APPROVED").cast("int")).alias("Approved"),
                         F.sum((col("peer_status") == "REJECTED").cast("int")).alias("Rejected")))

        table = (self.courses
                 .join(elig, "course_id", "left").join(subs, "course_id", "left")
                 .join(assigned, "course_id", "left").join(actioned, "course_id", "left")
                 .fillna(0, ["Eligible", "Submissions", "Total Peers", "Approved", "Rejected", "No Action"])
                 .select(col("phase").alias("Phase"), col("course_id").alias("Course ID"),
                         col("course_name").alias("Course"),
                         "Eligible", "Submissions", "Total Peers", "Approved", "Rejected", "No Action"))
        phase_totals = (table.groupBy("Phase")
                        .agg(F.lit("").alias("Course ID"), F.concat(col("Phase"), F.lit(" Total")).alias("Course"),
                             F.lit(None).cast("long").alias("Eligible"),
                             *[F.sum(c).alias(c) for c in ["Submissions", "Total Peers", "Approved", "Rejected", "No Action"]]))
        total = table.agg(F.lit("All").alias("Phase"), F.lit("").alias("Course ID"), F.lit("Total").alias("Course"),
                          F.lit(None).cast("long").alias("Eligible"),   # a user can be eligible for several courses
                          *[F.sum(c).alias(c) for c in ["Submissions", "Total Peers", "Approved", "Rejected", "No Action"]])
        return (table.orderBy("Phase", "Course")
                .unionByName(phase_totals.orderBy("Phase"))
                .unionByName(total))

    # ------------------------------------------------------- netcore files
    def netcore(self, tasks):
        """tasks: user_id, form_id, done (0/1) -> one row per user in Netcore layout.
           Status and phase always use ALL of the user's tasks."""
        phase_of = self.forms.select("form_id", "phase")
        per_user = (tasks.join(phase_of, "form_id")
                    .groupBy("user_id")
                    .agg(F.collect_set("phase").alias("phases"),
                         F.count("*").alias("assigned"),
                         F.sum("done").alias("done")))
        return (per_user.join(self.users, "user_id", "left")
                .select(col("user_id").alias("USERID"),
                        col("ministry_name").alias("PARENT_MINISTRY"),
                        col("dept_name").alias("PARENT_DEPARTMENT"),
                        col("userOrgName").alias("ORGANISATION"),
                        F.when(F.size("phases") > 1, "Both").otherwise(F.element_at("phases", 1)).alias("PHASE_FLAG"),
                        F.when(col("done") >= col("assigned"), "Completed").otherwise("Not Completed")
                        .alias("COMPLETION_STATUS"))
                .select(*NETCORE_COLS)
                .cache())

    def reviewer_tasks(self):
        # one task per (reviewer, submission); activity = assigned (submission date) or reviewed
        base = self.peers.select(col("peer_id").alias("user_id"), "form_id", col("review_done").alias("done"),
                                 "submitted_on", "reviewed_on")
        return (base.select("user_id", "form_id", "done", col("submitted_on").alias("activity_on"))
                .unionByName(base.filter(col("reviewed_on").isNotNull())
                             .select("user_id", "form_id", F.lit(None).cast("int").alias("done"),
                                     col("reviewed_on").alias("activity_on"))))

    def survey_tasks(self):
        # one task per (user, form) received; done = submitted. activity = became eligible or submitted
        submitted = (self.subs.groupBy(col("submittedBy").alias("user_id"), col("formId").alias("form_id"))
                     .agg(F.min("submitted_on").alias("submitted_on")))
        tasks = (self.eligible.select("user_id", "form_id", "completed_on")
                 .join(submitted, ["user_id", "form_id"], "outer")
                 .withColumn("done", col("submitted_on").isNotNull().cast("int")))
        return (tasks.select("user_id", "form_id", "done", col("completed_on").alias("activity_on"))
                .unionByName(tasks.filter(col("submitted_on").isNotNull())
                             .select("user_id", "form_id", F.lit(None).cast("int").alias("done"),
                                     col("submitted_on").alias("activity_on"))))

    # -------------------------------------------------------------- one scope
    def write_scope(self, out_dir, win, label):
        os.makedirs(out_dir, exist_ok=True)
        print(f"\n[{label}]", flush=True)

        table = self.summary_table(win)                       # always built: it goes into the mail
        if GENERATE_SUMMARY_CSV:
            write_csv(table, out_dir, "summary_table")

        netcore_files, netcore_counts = [], None
        if GENERATE_NETCORE:
            reviewers = self._netcore_with_activity(self.reviewer_tasks(), win)
            survey_users = self._netcore_with_activity(self.survey_tasks(), win)
            write_csv(reviewers, out_dir, "Reviewers")
            write_csv(survey_users, out_dir, "Survey users")
            netcore_files = [os.path.join(out_dir, "Reviewers.csv"), os.path.join(out_dir, "Survey users.csv")]
            netcore_counts = {
                "Users completed the survey": survey_users.filter(col("COMPLETION_STATUS") == "Completed").count(),
                "Reviewers tagged against survey": reviewers.count(),
            }

        responses = self.responses.filter(in_window("submitted_on", win))
        totals = table.filter(col("Course") == "Total").first()
        result = {"label": label, "win": win, "dir": out_dir, "files": [],
                  "netcore_files": netcore_files, "netcore_counts": netcore_counts,
                  "table": [r.asDict() for r in table.collect()],
                  "tiles": {"Total Submissions": int(totals["Submissions"] or 0),
                            "Peer Reviews Assigned": int(totals["Total Peers"] or 0),
                            "Peer Review Responses": responses.count()}}

        # user responses: answers submitted in the period
        write_csv(responses
                  .select("form_id", "course_id", "user_id", "submitted_on", "question",
                          col("answer").alias("response"))
                  .orderBy("form_id", "user_id", "submitted_on"),
                  out_dir, "user_responses", quote_all=True)
        # peer responses: peers assigned on submissions made in the period, or who reviewed in the period
        peers = self.peers if win is None else self.peers.filter(
            in_window("submitted_on", win) | in_window("reviewed_on", win))
        write_csv(peers
                  .select("form_id", "course_id", "submitter_id", "submitted_on", "peer_id",
                          F.coalesce(col("peer_status"), F.lit("NA")).alias("peer_status"),
                          "reviewed_on")
                  .orderBy("form_id", "submitter_id", "peer_id"),
                  out_dir, "peer_responses", quote_all=True)
        result["files"] = [os.path.join(out_dir, "user_responses.csv"),
                           os.path.join(out_dir, "peer_responses.csv")]
        return result

    def _netcore_with_activity(self, tasks, win):
        """Rows with done = NULL only carry an extra activity date (e.g. reviewed / submitted on);
           status is counted from the real task rows. Week file = users with any activity in the week."""
        out = self.netcore(tasks.filter(col("done").isNotNull()))
        if win is not None:
            active = (tasks.filter(in_window("activity_on", win))
                      .select(col("user_id").alias("USERID")).distinct())
            out = out.join(active, "USERID", "left_semi").select(*NETCORE_COLS)
        return out

    # -------------------------------------------------------------------- run
    def run(self, week_from: date, week_to: date):
        run_date = date.today().isoformat()
        base = os.path.join(OUT_BASE, run_date)
        print(f"Run {run_date} | week {week_from} .. {week_to} | output {base}", flush=True)
        self.load()
        cumulative = None
        if GENERATE_CUMULATIVE:
            cumulative = self.write_scope(os.path.join(base, "cumulative"), None, f"Cumulative (till {run_date})")
        week = self.write_scope(os.path.join(base, f"week_{week_from}_to_{week_to}"), (week_from, week_to),
                                f"Week ({week_from} to {week_to})")
        print("\nDone.", flush=True)
        # cumulative strip for the mail: everything from launch up to the last day of the week
        till = (date(2000, 1, 1), week_to)
        week["cum_tiles"] = {
            "Total Submissions": self.subs.filter(in_window("submitted_on", till))
            .select("submission_id").distinct().count(),
            "Peer Reviews Assigned": self.peers.filter(in_window("submitted_on", till)).count(),
            "Peer Review Responses": self.responses.filter(in_window("submitted_on", till)).count(),
        }
        print(f"  cumulative till {week_to}: {week['cum_tiles']}", flush=True)
        return base, cumulative, week


# ============================================================================
# EMAIL  (weekly numbers only, same layout as the Peer Validation report card)
# ============================================================================
NUM_COLS = ["Eligible", "Submissions", "Total Peers", "Approved", "Rejected", "No Action"]
NUM_COLOR = {"Approved": "#1a8f3c", "Rejected": "#d9480f", "No Action": "#c92a2a"}


def fmt_in(v):
    """Indian digit grouping: 2502543 -> 25,02,543; None -> em dash."""
    if v is None:
        return "&mdash;"
    s = str(int(v))
    if len(s) <= 3:
        return s
    head, tail = s[:-3], s[-3:]
    parts = []
    while len(head) > 2:
        parts.insert(0, head[-2:])
        head = head[:-2]
    if head:
        parts.insert(0, head)
    return ",".join(parts) + "," + tail


def mail_html(week):
    wf, wt = week["win"]
    period = f"{wf.strftime('%d %b %Y')} &ndash; {wt.strftime('%d %b %Y')}"
    t = week["tiles"]

    def tile(value, label, bg, border, color):
        return (f'<td width="33%" style="padding:6px"><div style="background:{bg};border:1px solid {border};'
                f'border-radius:8px;padding:14px 8px;text-align:center">'
                f'<div style="font-size:24px;font-weight:bold;color:{color}">{fmt_in(value)}</div>'
                f'<div style="font-size:12px;color:#444;margin-top:4px">{label}</div></div></td>')

    def strip(title, vals):
        return (f'<div style="font-size:13px;font-weight:bold;color:#0b2545;margin:14px 6px 0">{title}</div>'
                f'<table width="100%" cellspacing="0" cellpadding="0" style="margin-top:4px"><tr>'
                f'{tile(vals["Total Submissions"], "Total Submissions", "#fff4e6", "#ffd8a8", "#c2410c")}'
                f'{tile(vals["Peer Reviews Assigned"], "Peer Reviews Assigned", "#f6f8fb", "#dee2e6", "#0b2545")}'
                f'{tile(vals["Peer Review Responses"], "Peer Review Responses", "#ebfbee", "#b2f2bb", "#1a8f3c")}'
                f'</tr></table>')

    nc = week.get("netcore_counts")
    netcore_strip = "" if not nc else (
        f'<div style="font-size:13px;font-weight:bold;color:#0b2545;margin:14px 6px 0">'
        f'Survey users &amp; reviewers (this week)</div>'
        f'<table width="100%" cellspacing="0" cellpadding="0" style="margin-top:4px"><tr>'
        f'{tile(nc["Users completed the survey"], "Users completed the survey", "#e7f5ff", "#a5d8ff", "#1864ab")}'
        f'{tile(nc["Reviewers tagged against survey"], "Reviewers tagged against survey", "#f3f0ff", "#d0bfff", "#5f3dc4")}'
        f'</tr></table>')

    th = 'style="background:#0b2545;color:#fff;padding:10px;font-size:13px;text-align:{a}"'
    head = (f'<th {th.format(a="left")}>Course</th>' +
            "".join(f'<th {th.format(a="right")}>{c if c != "Submissions" else "Subm."}</th>' for c in NUM_COLS))

    rows = week["table"]
    body = ""
    for phase in ["Phase 1", "Phase 2"]:
        body += (f'<tr><td colspan="7" style="background:#fff4e6;color:#d9480f;font-weight:bold;'
                 f'padding:8px 10px;font-size:13px">{phase}</td></tr>')
        courses = [r for r in rows if r["Phase"] == phase and "Total" not in str(r["Course"])]
        for i, r in enumerate(courses):
            bg = "#f6f8fb" if i % 2 else "#ffffff"
            body += f'<tr style="background:{bg}"><td style="padding:8px 10px;font-size:13px">{r["Course"]}</td>'
            for c in NUM_COLS:
                weight = "bold" if c in ("Submissions", "Total Peers") else "normal"
                body += (f'<td style="padding:8px 10px;font-size:13px;text-align:right;font-weight:{weight};'
                         f'color:{NUM_COLOR.get(c, "#222")}">{fmt_in(r[c])}</td>')
            body += "</tr>"
        sub = next((r for r in rows if r["Course"] == f"{phase} Total"), None)
        if sub:
            body += f'<tr style="background:#e9edf3;font-weight:bold"><td style="padding:8px 10px;font-size:13px">{phase} Total</td>'
            body += "".join(f'<td style="padding:8px 10px;font-size:13px;text-align:right;color:{NUM_COLOR.get(c, "#222")}">'
                            f'{fmt_in(sub[c])}</td>' for c in NUM_COLS) + "</tr>"
    total = next(r for r in rows if r["Course"] == "Total")
    body += f'<tr style="background:#dde3ec;font-weight:bold"><td style="padding:10px;font-size:14px">Total</td>'
    body += "".join(f'<td style="padding:10px;font-size:14px;text-align:right;color:{NUM_COLOR.get(c, "#222")}">'
                    f'{fmt_in(total[c])}</td>' for c in NUM_COLS) + "</tr>"

    return f"""<div style="font-family:Arial,Helvetica,sans-serif;max-width:720px;color:#222">
<p style="font-size:14px">Dear Sir/Ma'am,</p>
<p style="font-size:14px">Please find enclosed here with the Peer Validation Weekly Report and the files attached.</p>
<table width="100%" cellspacing="0" cellpadding="0"><tr>
<td style="background:#d9480f;background-image:linear-gradient(90deg,#c2410c,#f08c00);border-radius:10px 10px 0 0;padding:16px 20px">
<span style="color:#fff;font-size:20px;font-weight:bold">Peer Validation</span>
<span style="color:#fff;font-size:13px;font-weight:bold;float:right;padding-top:5px">Week: {period}</span></td></tr></table>
{strip(f"Cumulative (since launch, till {wt.strftime('%d %b %Y')})", week["cum_tiles"]) if week.get("cum_tiles") else ""}
{strip(f"This week ({period})", t)}
{netcore_strip}
<h3 style="color:#0b2545;margin:18px 0 8px">Course-wise Adoption</h3>
<table width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse">
<tr>{head}</tr>{body}</table>
</div>"""


def send_mail(subject, html_body, attachments=None, to=None, cc=None):
    to = MAIL_TO if to is None else to
    cc = MAIL_CC if cc is None else cc
    if not (to and SMTP_USER and SMTP_PASSWORD):
        print("[MAIL] PV_MAIL_TO / PV_SMTP_USER / PV_SMTP_PASSWORD not set - skipping email", flush=True)
        return
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, MAIL_FROM, ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    msg.set_content("This report is best viewed in an HTML-capable mail client.")
    msg.add_alternative(html_body, subtype="html")
    for path in attachments or []:
        maintype, subtype = ("text", "csv") if path.endswith(".csv") else ("application", "zip")
        with open(path, "rb") as f:
            msg.add_attachment(f.read(), maintype=maintype, subtype=subtype, filename=os.path.basename(path))
    for attempt in (1, 2):
        try:
            if SMTP_PORT == 465:
                server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=300)
            else:
                server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=300)
                server.starttls()
            with server:
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.send_message(msg, to_addrs=to + cc)
            break
        except (smtplib.SMTPServerDisconnected, TimeoutError, OSError) as e:
            if attempt == 2:
                raise
            print(f"[MAIL] attempt {attempt} failed ({e}); retrying in 30s", flush=True)
            time.sleep(30)
    print(f"[MAIL] sent to {to + cc}", flush=True)


def make_zip(zip_base, files, rename=None):
    """Zip `files` into zip_base.zip. rename(path) -> name inside the zip (default: same name)."""
    tmp = zip_base + "_tmp"
    os.makedirs(tmp, exist_ok=True)
    for path in files:
        shutil.copyfile(path, os.path.join(tmp, rename(path) if rename else os.path.basename(path)))
    archive = shutil.make_archive(zip_base, "zip", root_dir=tmp)
    shutil.rmtree(tmp)
    print(f"[MAIL] attachment {os.path.basename(archive)} = {os.path.getsize(archive) / 1e6:.1f} MB", flush=True)
    return archive


def report_mail(base, week):
    wf, wt = week["win"]
    tag = f"{wf.strftime('%d%b')}_{wt.strftime('%d%b%Y')}"

    # zip 1: user / peer responses  (user_responses_19Oct_25Oct2026.csv, ...)
    zips = [make_zip(os.path.join(week["dir"], f"peer_validation_files_{tag}"), week["files"],
                     rename=lambda p: os.path.basename(p).replace(".csv", f"_{tag}.csv"))]
    # zip 2: Netcore files - names kept EXACTLY "Reviewers.csv" / "Survey users.csv"
    if week.get("netcore_files"):
        zips.append(make_zip(os.path.join(week["dir"], f"netcore_files_{tag}"), week["netcore_files"]))

    files, too_big = [], []
    for z in zips:
        (too_big if os.path.getsize(z) / 1e6 > MAX_ATTACH_MB else files).append(z)
    if sum(os.path.getsize(f) for f in files) / 1e6 > MAX_ATTACH_MB:      # together over the limit: keep the first
        too_big += files[1:]
        files = files[:1]

    note = ""
    if too_big:
        names = ", ".join(os.path.basename(z) for z in too_big)
        note = (f'<p style="font-size:13px;color:#c92a2a">{names} could not be attached because of the mail size '
                f'limit. They will be shared separately.</p>')

    subject = (MAIL_SUBJECT.replace("{from}", wf.strftime("%d %b"))
               .replace("{to}", wt.strftime("%d %b %Y")))
    body = mail_html(week)
    if note:
        body = body[:-len("</div>")] + note + "</div>"
    send_mail(subject, body, files)


def failure_mail(error_text):
    """Never goes to the report recipients. Only sent if PV_ALERT_TO is set."""
    if not ALERT_TO:
        print("[MAIL] job failed - no failure mail sent (PV_ALERT_TO not set). See this log.", flush=True)
        return
    body = (f"<p>The Peer Validation weekly job failed on {date.today().isoformat()}.</p>"
            f"<pre style='font-size:12px'>{html.escape(error_text[-5000:])}</pre>")
    try:
        send_mail(f"[FAILED] Peer Validation Weekly Report - {date.today().isoformat()}", body, to=ALERT_TO, cc=[])
    except Exception as e:
        print(f"[MAIL] could not send failure alert: {e}", flush=True)


def parse_args():
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)       # Monday of the previous week
    p = argparse.ArgumentParser()
    p.add_argument("--week-from", default=last_monday.isoformat(), help="YYYY-MM-DD (inclusive)")
    p.add_argument("--week-to", default=(last_monday + timedelta(days=6)).isoformat(), help="YYYY-MM-DD (inclusive)")
    p.add_argument("--no-email", action="store_true", help="build the files but do not send the mail")
    a = p.parse_args()
    return (datetime.strptime(a.week_from, "%Y-%m-%d").date(),
            datetime.strptime(a.week_to, "%Y-%m-%d").date(), a.no_email)


def main():
    week_from, week_to, no_email = parse_args()
    os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                         "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0,"
                                         "org.postgresql:postgresql:42.6.0 pyspark-shell")
    spark = (SparkSession.builder.appName("Peer Validation Report")
             .config("spark.sql.shuffle.partitions", "200")
             .config("spark.driver.memory", "30g")
             .config("spark.driver.maxResultSize", "3g")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
             .getOrCreate())
    start = datetime.now()
    try:
        config = create_config(get_environment_config())
        base, cumulative, week = PeerValidationReport(spark, config).run(week_from, week_to)
        if not no_email:
            report_mail(base, week)
    except Exception:
        err = traceback.format_exc()
        print(err, flush=True)
        if not no_email:
            failure_mail(err)
        spark.stop()
        sys.exit(1)
    print(f"[INFO] Total duration: {datetime.now() - start}")
    spark.stop()


if __name__ == "__main__":
    main()
