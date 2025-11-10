import re
import logging
from datetime import datetime, timezone
from time import time
import pandas as pd
from gmail_utils import parse_date_dynamic
from llm_utils import score_match_with_gemini

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def make_aware(dt):
    if dt is None:
        return None
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_domain(email_address: str) -> str:
    match = re.search(r'@([\w\.-]+)', email_address)
    return match.group(1).lower() if match else ""


def normalize_text(text: str) -> str:
    return re.sub(r'\s+', ' ', str(text).lower().strip())


def extract_amounts(text: str) -> list:
    amounts = []
    for m in re.findall(r'[$]?\s*[\d,]+(?:\.\d{1,2})?', text):
        try:
            a = float(m.replace("$", "").replace(",", "").strip())
            if 0.01 <= a <= 1000000:
                amounts.append(a)
        except:
            pass
    return amounts


def filter_emails(txn: dict, emails: list, date_window: int = 3, min_matches: int = 1) -> list:
    start_time = time()
    
    vendor_name = normalize_text(txn.get("vendor_name", "") or txn.get("Vendor", "") or txn.get("VendorName", ""))
    vendor_domain = (txn.get("Vendor", "") or txn.get("Vendor Domain", "")).lower()
    description = normalize_text(txn.get("description", "") or txn.get("description", "") or txn.get("Memo", ""))
    invoice_number = str(txn.get("transaction_id", "") or txn.get("Invoice Number", "") or txn.get("TransactionID", "")).strip().upper()
    txn_amount = float(txn.get("amount", 0) or txn.get("amount", 0))
    txn_date = make_aware(parse_date_dynamic(txn.get("date") or txn.get("Date", "")))
    
    logger.info(f"Stage 1 Filter START - Transaction: {invoice_number}")
    logger.info(f"  Vendor: {txn.get('vendor') or 'N/A'}, Domain: {vendor_domain or 'N/A'}, Amount: ${txn_amount}, Description: {description[:30] if description else 'N/A'}")
    
    filtered = []
    criteria_stats = {
        "domain": 0,
        "vendor_keyword": 0,
        "description_keyword": 0,
        "invoice_exact": 0,
        "invoice_numeric": 0,
        "amount_match": 0,
        "date_proximity": 0
    }
    
    for email in emails:
        email_from = str(email.get("from", "")).lower()
        email_subject = normalize_text(email.get("subject", ""))
        email_body = normalize_text(email.get("body", ""))
        email_snippet = normalize_text(email.get("snippet", ""))
        email_domain = extract_domain(email_from)
        email_date = make_aware(parse_date_dynamic(email.get("date")))
        
        searchable = f"{email_from} {email_subject} {email_body} {email_snippet}"
        
        match_score = 0
        match_reasons = []
        
        if vendor_domain and vendor_domain == email_domain:
            match_score += 1
            match_reasons.append("domain_match")
            criteria_stats["domain"] += 1
        
        if vendor_name and len(vendor_name) > 3:
            vendor_core = vendor_name.split()[0] if ' ' in vendor_name else vendor_name
            if len(vendor_core) > 3 and vendor_core in searchable:
                match_score += 1
                match_reasons.append(f"vendor:{vendor_core}")
                criteria_stats["vendor_keyword"] += 1
        
        if description and len(description) > 5:
            desc_words = [w for w in description.split() if len(w) > 4]
            for word in desc_words[:3]:
                if word in searchable:
                    match_score += 0.5
                    match_reasons.append(f"desc:{word}")
                    criteria_stats["description_keyword"] += 1
                    break
        
        if invoice_number and len(invoice_number) > 2:
            invoice_pattern = re.escape(invoice_number)
            if re.search(invoice_pattern, searchable.upper()):
                match_score += 3
                match_reasons.append("invoice_exact")
                criteria_stats["invoice_exact"] += 1
            else:
                numeric_part = re.search(r'\d{3,}', invoice_number)
                if numeric_part:
                    num = numeric_part.group()
                    if num in searchable:
                        match_score += 2
                        match_reasons.append(f"invoice_num:{num}")
                        criteria_stats["invoice_numeric"] += 1
        
        email_amounts = extract_amounts(searchable)
        for amount in email_amounts:
            if abs(amount - txn_amount) <= 0.01:
                match_score += 2
                match_reasons.append(f"amount_exact:${amount}")
                criteria_stats["amount_match"] += 1
                break
            elif abs(amount - txn_amount) <= 5:
                match_score += 1
                match_reasons.append(f"amount_close:${amount}")
                criteria_stats["amount_match"] += 1
                break
        
        if txn_date and email_date:
            days_diff = abs((txn_date - email_date).days)
            if days_diff <= date_window:
                match_score += 1
                match_reasons.append(f"date_{days_diff}d")
                criteria_stats["date_proximity"] += 1
        
        if match_score >= min_matches:
            email_copy = email.copy()
            email_copy['filter_score'] = match_score
            email_copy['filter_reasons'] = match_reasons
            filtered.append(email_copy)
    
    filtered.sort(key=lambda x: x['filter_score'], reverse=True)
    
    elapsed = time() - start_time
    logger.info(f"Stage 1 Filter COMPLETE - {len(emails)} → {len(filtered)} emails ({elapsed:.2f}s)")
    logger.info(f"  Criteria Stats: {criteria_stats}")
    
    return filtered


def score_with_gemini(txn: dict, filtered_emails: list, threshold: int = 60, max_emails: int = 10):
    start_time = time()
    
    transaction_id = txn.get("transaction_id")
    logger.info(f"Stage 2 Gemini START - Transaction: {transaction_id}")
    logger.info(f"  Processing top {min(len(filtered_emails), max_emails)} of {len(filtered_emails)} filtered emails")
    
    digest = []
    exceptions = []
    
    if not filtered_emails:
        logger.warning(f"  No filtered emails - adding to exceptions")
        exceptions.append({
            "TransactionID": transaction_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Reason": "No emails matched filter criteria",
        })
        return digest, exceptions
    
    best_match = None
    best_score = 0
    emails_processed = 0
    
    for email in filtered_emails[:max_emails]:
        emails_processed += 1
        email_subject = email.get("subject", "")[:50]
        
        try:
            logger.info(f"  Gemini call {emails_processed}/{max_emails} - Email: '{email_subject}...'")
            gemini_start = time()
            
            email_content = {
                "from": email.get("from"),
                "subject": email.get("subject"),
                "body": email.get("body", email.get("snippet", "")),
                "date": email.get("date"),
                "attachments": email.get("attachments", [])
            }
            
            gemini_result = score_match_with_gemini(txn, email_content)
            gemini_elapsed = time() - gemini_start
            
            score = gemini_result.get("score", 0)
            reason = gemini_result.get("reason", "")
            
            logger.info(f"    Score: {score}/100 ({gemini_elapsed:.2f}s) - {reason[:60]}")
            
            if score > best_score:
                best_score = score
                best_match = {
                    "email": email,
                    "score": score,
                    "reason": reason,
                    "gemini_data": gemini_result
                }
                logger.info(f"    ✓ New best match!")
        
        except Exception as e:
            logger.error(f"    ✗ Gemini error: {str(e)}")
            continue
    
    if best_match and best_score >= threshold:
        email = best_match["email"]
        logger.info(f"  ✓ MATCH FOUND - Score: {best_score} >= {threshold}")
        
        digest.append({
            "TransactionID": transaction_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Description": txn.get("description"),
            "EmailURL": email.get("gmail_url", ""),
            "EmailSender": email.get("from"),
            "EmailSubject": email.get("subject"),
            "MatchScore": round(best_score, 2),
            "MatchReason": best_match["reason"],
            "FilterReasons": ", ".join(email.get("filter_reasons", [])),
        })
    else:
        reason = f"Best match score {best_score:.1f} below threshold {threshold}" if best_match else "No confident match after LLM scoring"
        logger.warning(f"  ✗ NO MATCH - {reason}")
        
        exceptions.append({
            "TransactionID": transaction_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Description": txn.get("description"),
            "BestScore": round(best_score, 2) if best_match else 0,
            "Reason": reason,
        })
    
    elapsed = time() - start_time
    logger.info(f"Stage 2 Gemini COMPLETE - {elapsed:.2f}s total")
    
    return digest, exceptions


def hybrid_match(transactions: list, emails: list, threshold: int = 90, date_window: int = 3, 
                 min_matches: int = 2, max_emails_to_score: int = 10):
    logger.info("="*80)
    logger.info(f"HYBRID MATCH START")
    logger.info(f"  Transactions: {len(transactions)}")
    logger.info(f"  Total Emails: {len(emails)}")
    logger.info(f"  Config: threshold={threshold}, date_window={date_window}, min_matches={min_matches}, max_llm_calls={max_emails_to_score}")
    logger.info("="*80)
    
    overall_start = time()
    all_digest = []
    all_exceptions = []
    
    for idx, txn in enumerate(transactions):
        logger.info("")
        logger.info(f"{'='*90}")
        logger.info(f"Processing Transaction {idx+1}/{len(transactions)}")
        logger.info(f"{'='*90}")
        
        txn_start = time()
        
        filtered_emails = filter_emails(txn, emails, date_window, min_matches)
        
        digest, exceptions = score_with_gemini(txn, filtered_emails, threshold, max_emails_to_score)
        
        all_digest.extend(digest)
        all_exceptions.extend(exceptions)
        
        txn_elapsed = time() - txn_start
        logger.info(f"Transaction {idx+1} completed in {txn_elapsed:.2f}s")
    
    overall_elapsed = time() - overall_start
    logger.info("")
    logger.info("="*80)
    logger.info(f"HYBRID MATCH COMPLETE")
    logger.info(f"  Total Time: {overall_elapsed:.2f}s ({overall_elapsed/60:.1f} minutes)")
    logger.info(f"  Avg per Transaction: {overall_elapsed/len(transactions):.2f}s")
    logger.info(f"  Matches Found: {len(all_digest)}")
    logger.info(f"  Exceptions: {len(all_exceptions)}")
    logger.info("="*80)
    
    return all_digest, all_exceptions