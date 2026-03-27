#!/usr/bin/env python
# coding: utf-8

"""
Unified Postprocessing Flags Pipeline
--------------------------------------
Flow:
  1. Create target table (anomaly.duplicate_ap_invoice)
  2. Alter target table to add all flag columns
  3. Update invoice amount from source
  4. Run flags one by one (each updates target table in-place):
     a. Overlapping data cleanup
     b. Reversal flag
     c. Same invoice date flag
     d. Supplier error flag
     e. Keying error flag
     f. Date in invoice number flag (SAME_DATES_FLAG)
     g. New matched record number flag
  5. Final target table is ready with all flag values
"""

import pandas as pd
import numpy as np
from datetime import datetime
from itertools import combinations
from tqdm import tqdm
import Levenshtein
import re
import urllib
import sys
import gc
from sqlalchemy import create_engine, text

tqdm.pandas()


# ============================================================
# Database connection helper
# ============================================================

def get_engine(user="admin", pwd="Gpoproddb!#!", schema="anomaly",
               ip="prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"):
    password = urllib.parse.quote_plus(pwd)
    engine = create_engine(
        'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema)
    )
    return engine


def execute_sql(engine, sql):
    """Execute a SQL statement (DDL / DML)."""
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()


def read_table(engine, query="SELECT * FROM anomaly.duplicate_ap_invoice"):
    """Read data from the database."""
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df


def batch_update_flag(engine, table, flag_column, seq_no_values, flag_value):
    """Update a flag column for a list of seq_no values."""
    if not seq_no_values:
        return
    chunk_size = 500
    for i in range(0, len(seq_no_values), chunk_size):
        chunk = seq_no_values[i:i + chunk_size]
        placeholders = ",".join(["'{}'".format(str(s).replace("'", "''")) for s in chunk])
        sql = "UPDATE {t} SET `{col}` = {val} WHERE seq_no IN ({ids})".format(
            t=table, col=flag_column, val=flag_value, ids=placeholders
        )
        execute_sql(engine, sql)


# ============================================================
# STEP 1: Create target table
# ============================================================

def step1_create_target_table(engine):
    print("STEP 1: Creating target table ...")
    execute_sql(engine, "DROP TABLE IF EXISTS anomaly.duplicate_ap_invoice")
    sql = """
    CREATE TABLE anomaly.duplicate_ap_invoice (
      `seq_no` text,
      `Matched_Record_Number` bigint DEFAULT NULL,
      `Pay_Amount2` double DEFAULT NULL,
      `Invoice_Number` text,
      `Supplier` text,
      `Supplier_ID` text,
      `Supplier_Invoice_Number` text,
      `Created_On` date DEFAULT NULL,
      `Invoice_Date` date DEFAULT NULL,
      `External_PO_Number` text,
      `Check_Number` text,
      `Line_Description` text,
      `Settlement_Run_Number` text,
      `Extended Amount` double DEFAULT NULL,
      `Document_Link` text,
      `Document_Payment_Status` text,
      `Payment_Type` text,
      `VENDOR_NAME_ALIAS` text,
      `Priority_to_Validate` bigint DEFAULT NULL,
      `Reason_Grouped` text,
      `Iteration` text,
      `Confirmed` text,
      `Impact_Flag` text,
      `ConfirmedSpend` text,
      `confirmedSpendStatus` text,
      `Recovery_Logics` text,
      `MonthShort` text,
      `MonthNo` text,
      `Year` text,
      `user_name` text,
      `file_name` text,
      `execution_id` text,
      `bucket_flag` text,
      `reversal_flag` double DEFAULT NULL,
      `same_invoice_date_flag` double DEFAULT NULL,
      `date_in_invoice_number_flag` bigint DEFAULT NULL,
      `sequential_invoices_flag` double DEFAULT NULL,
      `location_flag` double DEFAULT NULL,
      `location` text,
      `line_description_flag` double DEFAULT NULL,
      `memo_description_flag` double DEFAULT NULL,
      `memo` text,
      `sequential_invoices_flag2` text,
      `levenshtein_distance` text
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """
    execute_sql(engine, sql)
    print("  -> Target table created.")


# ============================================================
# STEP 2: Alter target table – add all flag columns
# ============================================================

def step2_alter_target_table(engine):
    print("STEP 2: Altering target table to add flag columns ...")
    alter_columns = [
        "`keying_error_flag` DOUBLE DEFAULT NULL",
        "`supplier_error_flag` TEXT",
        "`VENDOR_NAME_ALIAS2` TEXT",
        "`new_matched_record_number` TEXT",
        "`cancel_flag` TEXT",
        "`Invoice_amount` DOUBLE DEFAULT NULL",
        "`flag_invoiceno_no_sequence` TEXT",
        "`flag_similar_descriptions` TEXT",
        "`flag_equal_amount` TEXT",
        "`flag_invoiceno_diff_within_1000` TEXT",
        "`Key_void` TEXT",
        "`Key_reversal` TEXT",
        "`VOID_FLAG` TEXT",
        "`Priority_reason_base` TEXT",
        "`Priority_flag_base` BIGINT DEFAULT NULL",
        "`Priority_reason_additional` TEXT",
        "`Priority_flag_additional` BIGINT DEFAULT NULL",
        "`payment_status` bigint DEFAULT NULL",
        "`incomplete_invoice_flag` varchar(45) DEFAULT NULL",
        "`flag_invoice_amount_variation` int NOT NULL DEFAULT '0'",
        "`reversal_group_number` bigint DEFAULT NULL",
        "`reversal_group_flag` varchar(45) DEFAULT NULL",
    ]
    for col_def in alter_columns:
        try:
            sql = "ALTER TABLE anomaly.duplicate_ap_invoice ADD COLUMN {}".format(col_def)
            execute_sql(engine, sql)
        except Exception as e:
            # Column may already exist
            if "Duplicate column" in str(e):
                pass
            else:
                print("  Warning adding column {}: {}".format(col_def.split('`')[1], e))
    print("  -> All flag columns added.")


# ============================================================
# STEP 3: Update invoice amount
# ============================================================

def step3_update_invoice_amount(engine):
    print("STEP 3: Updating invoice amount ...")
    sql = """
    UPDATE anomaly.duplicate_ap_invoice a
    INNER JOIN l1_t0004_db.temp_ap_inv b
    ON a.SEQ_NO = b.SEQ_NO
    SET a.INVOICE_AMOUNT = b.INVOICE_AMOUNT
    """
    execute_sql(engine, sql)
    print("  -> Invoice amount updated.")


# ============================================================
# STEP 4a: Overlapping data cleanup
# ============================================================

def step4a_overlapping_data(engine):
    print("STEP 4a: Overlapping data cleanup ...")
    df = read_table(engine)
    print("  Initial rows:", df.shape[0])

    report_rows = []
    for mrn in tqdm(df['Matched_Record_Number'].unique(), desc="  Building overlap report"):
        subset = df[df['Matched_Record_Number'] == mrn]
        subset_sorted = subset.sort_values(['seq_no'])
        row = {
            'Matched_Record_Number': mrn,
            'seq_no': subset_sorted['seq_no'].tolist()
        }
        report_rows.append(row)
    report = pd.DataFrame(report_rows)

    report['key'] = report.apply(lambda row: tuple(row['seq_no']), axis=1)
    report['Overlap'] = report.groupby('key', sort=False).ngroup()
    report.drop(columns=['key'], inplace=True)

    group = report['Overlap'].value_counts()
    valid_groups = group[group >= 2].index
    report = report[report['Overlap'].isin(valid_groups)].reset_index(drop=True).sort_values(by=['Overlap'])
    report['DELETE_OVERLAP'] = report.duplicated(subset=['Overlap'], keep='first').astype(int)

    df = df.merge(report[['Matched_Record_Number', 'Overlap', 'DELETE_OVERLAP']],
                  on='Matched_Record_Number', how='left')

    # Update Reason_Grouped for overlapping groups
    for mrn in tqdm(df['Overlap'].unique(), desc="  Updating Reason_Grouped"):
        if pd.notnull(mrn):
            subset = df[df['Overlap'] == mrn]
            df.loc[df[df['Overlap'] == mrn].index, 'Reason_Grouped'] = \
                ', '.join(sorted(subset['Reason_Grouped'].unique()))

    df = df[df['DELETE_OVERLAP'] != 1].reset_index(drop=True)
    df.drop(columns=['Overlap', 'DELETE_OVERLAP'], inplace=True)
    print("  Rows after overlap removal:", df.shape[0])

    # Replace the target table with cleaned data
    def upload(output):
        eng = get_engine()
        output.to_sql('duplicate_ap_invoice', con=eng, if_exists='replace', index=False, chunksize=5000)
    upload(df)
    print("  -> Overlapping data cleaned.")
    del df
    gc.collect()


# ============================================================
# STEP 4b: Reversal flag
# ============================================================

def step4b_reversal_flag(engine):
    print("STEP 4b: Running reversal flag ...")

    # Drop temp tables
    for tbl in ['anomaly.duplicate_ap_invoice_temp3',
                'anomaly.duplicate_ap_invoice_temp4',
                'anomaly.duplicate_ap_invoice_temp5',
                'anomaly.update_negative_value_reversal']:
        execute_sql(engine, "DROP TABLE IF EXISTS {}".format(tbl))

    # Step 2: Create temp3
    execute_sql(engine, """
        CREATE TABLE anomaly.duplicate_ap_invoice_temp3 AS
        SELECT a.*, b.INVOICE_AMOUNT AS INVOICE_AMOUNT_temp
        FROM anomaly.duplicate_ap_invoice a
        INNER JOIN l1_t0004_db.temp_ap_inv b ON a.SEQ_NO = b.SEQ_NO
    """)

    # Step 3: Create temp4
    execute_sql(engine, """
        CREATE TABLE anomaly.duplicate_ap_invoice_temp4 AS
        SELECT SEQ_NO, SUPPLIERS_INVOICE_NUMBER,
            COALESCE(CAST(REGEXP_REPLACE(SUPPLIERS_INVOICE_NUMBER, '[^0-9]+', '') AS DECIMAL(65, 0)), -99)
                AS cleaned_Supplier_Invoice_Number,
            b.VENDOR_NAME_ALIAS,
            Supplier,
            INVOICE_AMOUNT,
            Invoice_Date
        FROM l1_t0004_db.temp_ap_inv a
        LEFT JOIN l3_dm_db.dim_vendor b ON a.Supplier = b.vendor_name
    """)

    # Step 4: Create temp5
    execute_sql(engine, """
        CREATE TABLE anomaly.duplicate_ap_invoice_temp5 AS
        SELECT DISTINCT seq_no, Matched_Record_Number, match_flag FROM (
            SELECT a.*,
                CASE
                    WHEN cleaned_Supplier_Invoice_Number = B_cleaned_Supplier_Invoice_Number THEN 1
                    WHEN INVOICE_AMOUNT < 0 AND DATEDIFF(Invoice_Date, B_Invoice_Date) >= 0 THEN 2
                    WHEN INVOICE_AMOUNT >= 0 AND DATEDIFF(B_Invoice_Date, Invoice_Date) >= 0 THEN 2
                    ELSE 0
                END AS match_flag
            FROM (
                SELECT DISTINCT a.*,
                    b.SEQ_NO AS B_SEQ_NO,
                    ROW_NUMBER() OVER(PARTITION BY a.SEQ_NO, a.Matched_Record_Number
                        ORDER BY DATEDIFF(a.Invoice_Date, b.Invoice_Date) ASC) AS rnk1,
                    b.Invoice_Date AS B_Invoice_Date,
                    b.cleaned_Supplier_Invoice_Number AS B_cleaned_Supplier_Invoice_Number,
                    b.INVOICE_AMOUNT AS B_INVOICE_AMOUNT
                FROM (
                    SELECT a.seq_no, a.Supplier_Invoice_Number,
                        COALESCE(CAST(REGEXP_REPLACE(Supplier_Invoice_Number, '[^0-9]+', '') AS DECIMAL(65, 0)), -99)
                            AS cleaned_Supplier_Invoice_Number,
                        VENDOR_NAME_ALIAS, Supplier, INVOICE_AMOUNT_temp AS INVOICE_AMOUNT,
                        Matched_Record_Number, Reason_Grouped, Invoice_Date
                    FROM anomaly.duplicate_ap_invoice_temp3 a
                ) a
                INNER JOIN anomaly.duplicate_ap_invoice_temp4 b
                    ON a.Supplier = b.Supplier
                    AND -(a.INVOICE_AMOUNT) = (b.INVOICE_AMOUNT)
                    AND a.SEQ_NO <> b.SEQ_NO
            ) a
            WHERE rnk1 = 1
        ) a
    """)

    # Step 5: Update reversal_flag from temp5
    execute_sql(engine, """
        UPDATE anomaly.duplicate_ap_invoice a
        INNER JOIN anomaly.duplicate_ap_invoice_temp5 b
            ON a.seq_no = b.seq_no
            AND a.Matched_Record_Number = b.Matched_Record_Number
        SET a.reversal_flag = b.match_flag
    """)

    # Step 6: Set NULL reversal_flag to 0
    execute_sql(engine, """
        UPDATE anomaly.duplicate_ap_invoice
        SET reversal_flag = 0
        WHERE reversal_flag IS NULL
    """)

    # Step 7-8: Create update_negative_value_reversal
    execute_sql(engine, """
        CREATE TABLE anomaly.update_negative_value_reversal AS
        SELECT seq_no FROM anomaly.duplicate_ap_invoice
        WHERE Matched_Record_Number IN (
            SELECT Matched_Record_Number FROM (
                SELECT DISTINCT Matched_Record_Number
                FROM anomaly.duplicate_ap_invoice
                WHERE reversal_flag <> 0
            ) a
        )
        AND reversal_flag = 0
    """)

    # Step 9: Update related records
    execute_sql(engine, """
        UPDATE anomaly.duplicate_ap_invoice a
        INNER JOIN anomaly.update_negative_value_reversal b
            ON a.SEQ_NO = b.SEQ_NO
        SET reversal_flag = -1
    """)

    # Cleanup temp tables
    for tbl in ['anomaly.duplicate_ap_invoice_temp3',
                'anomaly.duplicate_ap_invoice_temp4',
                'anomaly.duplicate_ap_invoice_temp5',
                'anomaly.update_negative_value_reversal']:
        execute_sql(engine, "DROP TABLE IF EXISTS {}".format(tbl))

    print("  -> Reversal flag updated.")


# ============================================================
# STEP 4c: Same invoice date flag
# ============================================================

def step4c_same_invoice_date_flag(engine):
    print("STEP 4c: Running same invoice date flag ...")
    sql = """
    UPDATE anomaly.duplicate_ap_invoice a
    SET a.same_invoice_date_flag = CASE
        WHEN EXISTS (
            SELECT 1 FROM (
                SELECT Matched_Record_Number
                FROM anomaly.duplicate_ap_invoice
                GROUP BY Matched_Record_Number
                HAVING COUNT(DISTINCT Invoice_Date) = 1
            ) t
            WHERE t.Matched_Record_Number = a.Matched_Record_Number
        )
        THEN 1 ELSE 0
    END
    """
    execute_sql(engine, sql)
    print("  -> Same invoice date flag updated.")


# ============================================================
# STEP 4d: Supplier error flag
# ============================================================

def step4d_supplier_error_flag(engine):
    print("STEP 4d: Running supplier error flag ...")
    sql = """
    UPDATE anomaly.duplicate_ap_invoice a
    INNER JOIN (
        SELECT DISTINCT Matched_Record_Number FROM (
            WITH anomaly_data AS (
                SELECT d.* FROM anomaly.duplicate_ap_invoice d
            )
            SELECT t1.* FROM anomaly_data t1
            JOIN anomaly_data t2
                ON t1.Supplier_Invoice_Number = t2.Supplier_Invoice_Number
                AND t1.Invoice_Date = t2.Invoice_Date
                AND t1.Matched_Record_Number = t2.Matched_Record_Number
                AND t1.INVOICE_AMOUNT = t2.INVOICE_AMOUNT
                AND t1.Supplier <> t2.Supplier
            ORDER BY t1.Supplier_Invoice_Number
        ) AS abc
        ORDER BY Supplier_Invoice_Number
    ) b ON a.Matched_Record_Number = b.Matched_Record_Number
    SET supplier_error_flag = 1
    """
    execute_sql(engine, sql)
    print("  -> Supplier error flag updated.")


# ============================================================
# STEP 4e: Keying error flag (Python-based, UPDATE in-place)
# ============================================================

def step4e_keying_error_flag(engine):
    print("STEP 4e: Running keying error flag ...")
    df = read_table(engine)
    df = df.reset_index(drop=True)

    keying_flag_seqnos = set()

    # --- Levenshtein on Supplier_Invoice_Number ---
    lev_map = {}
    for mrn in df['Matched_Record_Number'].unique():
        sub = df[df['Matched_Record_Number'] == mrn]
        uni = sub['Supplier_Invoice_Number'].dropna().unique()
        if len(uni) == 2:
            lev_dist = Levenshtein.distance(str(uni[0]), str(uni[1]))
            lev_map[mrn] = lev_dist

    # Levenshtein == 1 -> keying error
    for mrn, dist in lev_map.items():
        if dist == 1:
            seqs = df[df['Matched_Record_Number'] == mrn]['seq_no'].tolist()
            keying_flag_seqnos.update(seqs)

    # 2-digit transposition on Supplier_Invoice_Number
    for mrn, dist in lev_map.items():
        if dist == 2:
            sub = df[df['Matched_Record_Number'] == mrn]
            uni = sub['Supplier_Invoice_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])
                if len(s1) == len(s2):
                    diff_idx = [i for i in range(len(s1)) if s1[i] != s2[i]]
                    if (len(diff_idx) == 2 and
                            diff_idx[1] == diff_idx[0] + 1 and
                            s1[diff_idx[0]] == s2[diff_idx[1]] and
                            s1[diff_idx[1]] == s2[diff_idx[0]]):
                        seqs = sub['seq_no'].tolist()
                        keying_flag_seqnos.update(seqs)

    # --- Levenshtein on Check_Number ---
    lev_check_map = {}
    for mrn in df['Matched_Record_Number'].unique():
        sub = df[df['Matched_Record_Number'] == mrn]
        uni = sub['Check_Number'].dropna().unique()
        if len(uni) == 2:
            lev_dist = Levenshtein.distance(str(uni[0]), str(uni[1]))
            lev_check_map[mrn] = lev_dist

    for mrn, dist in lev_check_map.items():
        if dist == 1:
            seqs = df[df['Matched_Record_Number'] == mrn]['seq_no'].tolist()
            keying_flag_seqnos.update(seqs)

    for mrn, dist in lev_check_map.items():
        if dist == 2:
            sub = df[df['Matched_Record_Number'] == mrn]
            uni = sub['Check_Number'].dropna().unique()
            if len(uni) == 2:
                s1, s2 = str(uni[0]), str(uni[1])
                if len(s1) == len(s2):
                    diff_idx = [i for i in range(len(s1)) if s1[i] != s2[i]]
                    if (len(diff_idx) == 2 and
                            diff_idx[1] == diff_idx[0] + 1 and
                            s1[diff_idx[0]] == s2[diff_idx[1]] and
                            s1[diff_idx[1]] == s2[diff_idx[0]]):
                        seqs = sub['seq_no'].tolist()
                        keying_flag_seqnos.update(seqs)

    # --- Long common substring (LCS) check ---
    def max_consecutive_same(s1, s2):
        m, n = len(s1), len(s2)
        max_len = 0
        dp = [[0] * (n + 1) for _ in range(m + 1)]
        for i in range(m):
            for j in range(n):
                if s1[i] == s2[j]:
                    dp[i + 1][j + 1] = dp[i][j] + 1
                    if dp[i + 1][j + 1] > max_len:
                        max_len = dp[i + 1][j + 1]
        return max_len

    for mrn in df['Matched_Record_Number'].unique():
        sub = df[df['Matched_Record_Number'] == mrn]
        unique_suppl = sub['Supplier_Invoice_Number'].dropna().unique()
        if len(unique_suppl) == 2:
            s1, s2 = str(unique_suppl[0]), str(unique_suppl[1])
            lcs_len = max_consecutive_same(s1, s2)
            ratios = [lcs_len / len(x) if len(x) > 0 else 0 for x in [s1, s2]]
            cond1 = any(r == 1 for r in ratios)
            cond2 = (sub['Supplier_Invoice_Number'].nunique() > 1 and
                     any(r > 0.74 for r in ratios))
            cond3 = (sub['Supplier_Invoice_Number'].nunique() > 1 and lcs_len >= 8)
            if cond1 or cond2 or cond3:
                seqs = sub['seq_no'].tolist()
                keying_flag_seqnos.update(seqs)

    # Batch update keying_error_flag = 1
    keying_list = list(keying_flag_seqnos)
    print("  Keying error flag rows:", len(keying_list))
    batch_update_flag(engine, "anomaly.duplicate_ap_invoice", "keying_error_flag", keying_list, 1)

    del df
    gc.collect()
    print("  -> Keying error flag updated.")


# ============================================================
# STEP 4f: Date in invoice number flag (SAME_DATES_FLAG)
# ============================================================

def step4f_date_in_invoice_number_flag(engine):
    print("STEP 4f: Running date in invoice number flag ...")
    df = read_table(engine)
    df = df.reset_index()

    def extract_dates(text_val):
        if pd.isna(text_val):
            return None
        text_val = str(text_val).lower()
        if "e+" in text_val or "e-" in text_val:
            return None
        month_patterns = (
            r"((?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
            r"aug(?:ust)?|sep(?:tember)?|sept(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
            r"(\d{4})?(?=\b|[^a-zA-Z]|\d{4}))"
        )
        date_patterns = [
            r"(?<!\d)[^\d]?\(?\{?\[?"
            r"(\d{1,2}[-_/\\.\\]\d{1,2}[-_/\\.\\]\d{2,4}|\d{2,4}[-_/\\.\\]\d{1,2}[-_/\\.\\]\d{1,2})"
            r"\)?\}?\]?[^0-9]?(?!\d)",
            r"(?<!\d)[^\d]?\(?\{?\[?"
            r"(\d{1,2}[-_/\\.\\]\d{2,4}|\d{2,4}[-_/\\.\\]\d{1,2})"
            r"\)?\}?\]?[^0-9]?(?!\d)",
        ]
        for pattern in date_patterns:
            matches = re.findall(pattern, text_val)
            for match in matches:
                date_formats = [
                    "%d-%m-%Y", "%Y-%m-%d", "%Y-%d-%m", "%m-%d-%Y",
                    "%m-%Y", "%Y-%m", "%m-%d-%y", "%d-%m-%y", "%y-%m-%d",
                    "%y-%d-%m", "%m-%y", "%y-%m"
                ]
                match = re.sub(r"[\(\)\[\]\{\}\\\/\.]", "-", match)
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(match, fmt)
                        return parsed_date
                    except:
                        continue
        found_months = re.search(month_patterns, text_val, re.IGNORECASE)
        month_mapping = {
            "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
            "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
            "aug": 8, "august": 8, "sep": 9, "september": 9, "oct": 10, "october": 10,
            "nov": 11, "november": 11, "dec": 12, "december": 12
        }
        if found_months:
            month_key = re.sub(r"[^a-zA-Z]", "", found_months.group(0))[:3].lower()
            if month_key in month_mapping:
                return month_mapping[month_key]
        found_number_month = re.search(r"(?<![\d])(\d{2})(\d{4})?(?!\d)", text_val)
        if found_number_month:
            if len(found_number_month.group(0)) > 6:
                return 0
            else:
                year = int(found_number_month.group(2)) if found_number_month.group(2) else 0
                if year is not None and (year < 2020 or year > 2025):
                    return None
                month_number = int(found_number_month.group(1))
                if 1 <= month_number <= 12:
                    return month_number
            return None

    # Compute flag
    flag_pos = {}  # seq_no -> 1
    flag_neg = {}  # seq_no -> -1

    grouped = df.groupby(["Matched_Record_Number", 'Reason_Grouped', 'file_name'])
    print("  Groups:", len(grouped))

    for matching_number, group in grouped:
        if len(group) != 2:
            if group['Supplier_Invoice_Number'].nunique() == 2:
                group1 = group.copy()
                group1 = group1.drop_duplicates(subset=['Supplier_Invoice_Number'])
                group1['Extracted_Month'] = group1['Supplier_Invoice_Number'].apply(extract_dates)
                if group1['Extracted_Month'].notna().sum() == 2:
                    months = [x.month if isinstance(x, datetime) else x
                              for x in group1['Extracted_Month'].tolist()]
                    val = 1 if months[0] == months[1] else -1
                    for sn in group['seq_no'].tolist():
                        if val == 1:
                            flag_pos[sn] = 1
                        else:
                            flag_neg[sn] = -1
            continue

        group = group.copy()
        group['Extracted_Month'] = group['Supplier_Invoice_Number'].apply(extract_dates)
        if group['Extracted_Month'].notna().sum() == 2:
            months = [x.month if isinstance(x, datetime) else x
                      for x in group['Extracted_Month'].tolist()]
            val = 1 if months[0] == months[1] else -1
            for sn in group['seq_no'].tolist():
                if val == 1:
                    flag_pos[sn] = 1
                else:
                    flag_neg[sn] = -1

    # Set default to 0 for all
    execute_sql(engine, """
        UPDATE anomaly.duplicate_ap_invoice
        SET date_in_invoice_number_flag = 0
        WHERE date_in_invoice_number_flag IS NULL
    """)

    # Update flag = 1
    batch_update_flag(engine, "anomaly.duplicate_ap_invoice",
                      "date_in_invoice_number_flag", list(flag_pos.keys()), 1)
    # Update flag = -1
    batch_update_flag(engine, "anomaly.duplicate_ap_invoice",
                      "date_in_invoice_number_flag", list(flag_neg.keys()), -1)

    print("  Flag=1 count:", len(flag_pos))
    print("  Flag=-1 count:", len(flag_neg))

    del df
    gc.collect()
    print("  -> Date in invoice number flag updated.")


# ============================================================
# STEP 4g: New matched record number flag
# ============================================================

def step4g_new_matched_record_number_flag(engine):
    print("STEP 4g: Running new matched record number flag ...")

    # Compute new_matched_record_number via CTE and update
    execute_sql(engine, """
        UPDATE anomaly.duplicate_ap_invoice a
        INNER JOIN (
            WITH grouped_invoice_sets AS (
                SELECT Reason_Grouped, Matched_Record_Number,
                    GROUP_CONCAT(DISTINCT Supplier_Invoice_Number
                        ORDER BY Supplier_Invoice_Number SEPARATOR ',') AS invoice_combo
                FROM anomaly.duplicate_ap_invoice
                GROUP BY Reason_Grouped, Matched_Record_Number
            ),
            group_ranks AS (
                SELECT Reason_Grouped, invoice_combo,
                    DENSE_RANK() OVER (PARTITION BY Reason_Grouped ORDER BY invoice_combo)
                        AS new_mrn
                FROM (SELECT DISTINCT Reason_Grouped, invoice_combo FROM grouped_invoice_sets) AS combos
            )
            SELECT a.seq_no, a.Matched_Record_Number, b.new_mrn
            FROM anomaly.duplicate_ap_invoice a
            JOIN grouped_invoice_sets gis
                ON a.Reason_Grouped = gis.Reason_Grouped
                AND a.Matched_Record_Number = gis.Matched_Record_Number
            JOIN group_ranks b
                ON gis.Reason_Grouped = b.Reason_Grouped
                AND gis.invoice_combo = b.invoice_combo
        ) t ON a.seq_no = t.seq_no AND a.Matched_Record_Number = t.Matched_Record_Number
        SET a.new_matched_record_number = t.new_mrn
    """)

    # Apply offsets by Reason_Grouped
    reason_offsets = [
        ("Fuzzy", 100000000),
        ("Fuzzy, ML1.1", 110000000),
        ("Fuzzy, ML1.1, ML2", 11100000),
        ("Fuzzy, ML1.1, ML2, Same invoice details but different check details", 112000000),
        ("Fuzzy, ML1.1, Same invoice details but different check details", 113000000),
        ("Fuzzy, ML2", 114000000),
        ("Fuzzy, Same invoice details but different check details", 115000000),
        ("Fuzzy, Same supplier but discrepancies in invoice records", 116000000),
        ("ML1.1", 117000000),
        ("ML1.1, ML2", 118000000),
        ("ML1.1, ML2, Same Invoice but diffrent supplier", 119000000),
        ("ML1.1, ML2, Same invoice details but different check details", 120000000),
        ("ML1.1, Same Invoice but diffrent supplier", 121000000),
        ("ML1.1, Same invoice details but different check details", 122000000),
        ("ML1.1, Same invoice details but different check details, Same supplier but discrepancies in invoice records", 123000000),
        ("ML1.3", 124000000),
        ("ML1.3, Same Invoice but diffrent supplier", 125000000),
        ("ML2", 126000000),
        ("ML2, Same Invoice but diffrent supplier", 127000000),
        ("ML2, Same invoice details but different check details", 128000000),
        ("Same Invoice but diffrent supplier", 129000000),
        ("Same invoice details but different check details", 130000000),
        ("Same invoice details but different check details, Same supplier but discrepancies in invoice records", 131000000),
        ("Same supplier but discrepancies in invoice records", 132000000),
    ]
    for reason, offset in reason_offsets:
        sql = """
            UPDATE anomaly.duplicate_ap_invoice
            SET new_matched_record_number = new_matched_record_number + {offset}
            WHERE Reason_Grouped = '{reason}'
        """.format(offset=offset, reason=reason.replace("'", "''"))
        execute_sql(engine, sql)

    print("  -> New matched record number flag updated.")


# ============================================================
# MAIN: Run all steps in order
# ============================================================

def main():
    print("=" * 60)
    print("POSTPROCESSING FLAGS PIPELINE")
    print("=" * 60)

    engine = get_engine()

    # Step 1: Create target table
    #step1_create_target_table(engine)

    # Step 2: Alter target table – add all flag columns
    step2_alter_target_table(engine)

    # Step 3: Update invoice amount
    step3_update_invoice_amount(engine)

    # Step 4: Run all flags one by one
    step4a_overlapping_data(engine)
    step4b_reversal_flag(engine)
    step4c_same_invoice_date_flag(engine)
    step4d_supplier_error_flag(engine)
    step4e_keying_error_flag(engine)
    step4f_date_in_invoice_number_flag(engine)
    step4g_new_matched_record_number_flag(engine)

    print("=" * 60)
    print("PIPELINE COMPLETE")
    print("Target table anomaly.duplicate_ap_invoice is ready with all flags.")
    print("=" * 60)


if __name__ == "__main__":
    main()
