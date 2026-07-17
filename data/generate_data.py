#!/usr/bin/env python3
"""
Synthetic dataset generator for the Credicuotas Data Engineer challenge.

This script is committed for TRANSPARENCY only — you do NOT need to run it.
The challenge already ships the generated CSVs next to this file. It is
deterministic (fixed seed), so re-running it reproduces the exact same data.

Domain: a digital consumer-lending product. People apply for a loan
(loan_applications); approved applications get money disbursed (disbursements);
each disbursement is repaid in monthly installments / cuotas (installments).

The data INTENTIONALLY contains real-world quality issues (duplicates, nulls,
orphan foreign keys, bad amounts, mixed date formats). Handling them cleanly is
part of the challenge — see README.md.
"""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

HERE = Path(__file__).resolve().parent

N_CUSTOMERS = 300
N_APPLICATIONS = 800

PROVINCES = [
    "Buenos Aires", "CABA", "Córdoba", "Santa Fe", "Mendoza",
    "Tucumán", "Salta", "Entre Ríos", "Chaco", "Misiones",
]
SEGMENTS = ["MASS", "PLUS", "PREMIUM"]
PRODUCTS = ["PERSONAL", "CONSUMO", "TARJETA"]
CHANNELS = ["APP", "WEB", "SUCURSAL", "PARTNER"]
APP_STATUSES = ["APPROVED", "REJECTED", "PENDING"]

START = date(2025, 1, 1)
END = date(2025, 12, 31)


def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def iso(d: date) -> str:
    return d.isoformat()


# ---------------------------------------------------------------- customers
customers = []
for i in range(1, N_CUSTOMERS + 1):
    cust_id = f"CUST_{i:05d}"
    created = rand_date(date(2018, 1, 1), END)
    birth = rand_date(date(1960, 1, 1), date(2005, 12, 31))
    province = random.choice(PROVINCES)
    segment = random.choices(SEGMENTS, weights=[70, 22, 8])[0]
    # DQ issue: ~3% of customers have a missing province
    if random.random() < 0.03:
        province = ""
    customers.append({
        "customer_id": cust_id,
        "created_at": iso(created),
        "birth_date": iso(birth),
        "province": province,
        "segment": segment,
    })

# ------------------------------------------------------------- applications
applications = []
for i in range(1, N_APPLICATIONS + 1):
    app_id = f"APP_{i:06d}"
    cust = random.choice(customers)
    applied = rand_date(START, END)
    product = random.choice(PRODUCTS)
    channel = random.choices(CHANNELS, weights=[45, 25, 15, 15])[0]
    requested = random.choice([50000, 100000, 150000, 200000, 300000, 500000, 750000])
    term = random.choice([3, 6, 9, 12, 18, 24])
    score = random.randint(300, 950)
    # Approval is loosely correlated with score
    if score >= 650:
        status = random.choices(APP_STATUSES, weights=[75, 15, 10])[0]
    elif score >= 500:
        status = random.choices(APP_STATUSES, weights=[45, 45, 10])[0]
    else:
        status = random.choices(APP_STATUSES, weights=[15, 75, 10])[0]

    # DQ issue: ~2% have a missing requested_amount
    requested_out = "" if random.random() < 0.02 else requested
    # DQ issue: a few have a negative amount (data entry error)
    if random.random() < 0.01:
        requested_out = -requested

    applications.append({
        "application_id": app_id,
        "customer_id": cust["customer_id"],
        "applied_at": iso(applied),
        "product": product,
        "channel": channel,
        "requested_amount": requested_out,
        "term_months": term,
        "credit_score": score,
        "status": status,
        "_applied_date": applied,   # internal, not written
        "_term": term,              # internal, not written
        "_requested": requested,    # internal, not written
    })

# DQ issue: inject ~10 exact-duplicate application rows
for app in random.sample(applications, 10):
    applications.append(dict(app))

# ------------------------------------------------------------ disbursements
disbursements = []
disb_counter = 1
for app in applications:
    if app["status"] != "APPROVED":
        continue
    # ~95% of approved applications actually get disbursed
    if random.random() > 0.95:
        continue
    disb_id = f"DISB_{disb_counter:06d}"
    disb_counter += 1
    days_to_disb = random.randint(0, 21)
    disbursed_at = app["_applied_date"] + timedelta(days=days_to_disb)
    if disbursed_at > END:
        disbursed_at = END
    base = app["_requested"]
    # disbursed amount sometimes differs slightly from requested
    disbursed_amount = int(base * random.choice([1.0, 1.0, 1.0, 0.9, 0.8]))
    rate = round(random.uniform(0.45, 1.20), 4)  # annual nominal rate
    # DQ issue: ~3% of disbursed_at use a different date format (DD/MM/YYYY)
    if random.random() < 0.03:
        disbursed_at_str = disbursed_at.strftime("%d/%m/%Y")
    else:
        disbursed_at_str = iso(disbursed_at)
    disbursements.append({
        "disbursement_id": disb_id,
        "application_id": app["application_id"],
        "disbursed_at": disbursed_at_str,
        "disbursed_amount": disbursed_amount,
        "annual_interest_rate": rate,
        "term_months": app["_term"],
        "_disbursed_date": disbursed_at,
        "_amount": disbursed_amount,
        "_term": app["_term"],
    })

# DQ issue: 5 orphan disbursements pointing to non-existent applications
for k in range(5):
    disb_id = f"DISB_{disb_counter:06d}"
    disb_counter += 1
    d = rand_date(START, END)
    disbursements.append({
        "disbursement_id": disb_id,
        "application_id": f"APP_999{k:03d}",  # does not exist
        "disbursed_at": iso(d),
        "disbursed_amount": 100000,
        "annual_interest_rate": 0.75,
        "term_months": 12,
        "_disbursed_date": d,
        "_amount": 100000,
        "_term": 12,
    })

# ------------------------------------------------------------- installments
installments = []
inst_counter = 1
TODAY = date(2026, 1, 15)  # reference "current" date for delinquency
for disb in disbursements:
    term = disb["_term"]
    monthly = round(disb["_amount"] * (1 + disb["annual_interest_rate"] * (term / 12)) / term, 2)
    for n in range(1, term + 1):
        inst_id = f"INST_{inst_counter:07d}"
        inst_counter += 1
        due = disb["_disbursed_date"] + timedelta(days=30 * n)
        paid_at = None
        paid_amount = ""
        if due <= TODAY:
            # past-due installment: most are paid, some are delinquent
            roll = random.random()
            if roll < 0.80:                       # paid on time / early
                paid = due - timedelta(days=random.randint(0, 5))
                paid_at = iso(paid)
                paid_amount = monthly
            elif roll < 0.90:                     # paid late
                paid = due + timedelta(days=random.randint(1, 40))
                paid_at = iso(paid)
                paid_amount = monthly
            else:                                 # unpaid -> delinquent
                paid_at = ""
                paid_amount = ""
        else:
            # future installment: not due yet
            paid_at = ""
            paid_amount = ""
        installments.append({
            "installment_id": inst_id,
            "disbursement_id": disb["disbursement_id"],
            "installment_number": n,
            "due_date": iso(due),
            "due_amount": monthly,
            "paid_at": paid_at if paid_at is not None else "",
            "paid_amount": paid_amount,
        })


def write_csv(name: str, rows: list, fields: list):
    path = HERE / name
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in fields})
    print(f"  {name:28s} {len(rows):6d} rows")


print("Generating Credicuotas challenge dataset (seed=%d)..." % SEED)
write_csv("customers.csv", customers,
          ["customer_id", "created_at", "birth_date", "province", "segment"])
write_csv("loan_applications.csv", applications,
          ["application_id", "customer_id", "applied_at", "product", "channel",
           "requested_amount", "term_months", "credit_score", "status"])
write_csv("disbursements.csv", disbursements,
          ["disbursement_id", "application_id", "disbursed_at",
           "disbursed_amount", "annual_interest_rate", "term_months"])
write_csv("installments.csv", installments,
          ["installment_id", "disbursement_id", "installment_number",
           "due_date", "due_amount", "paid_at", "paid_amount"])
print("Done.")
