from typing import List, Dict, Any
from time import time

from app.rag_pipeline import INDEX_ROOT, csv_row_to_enhanced_query, format_results, global_search


RAG_THRESHOLD = 50.0  

def score_rag_transaction(txn: dict, rag_results: List[Dict[str, Any]], threshold: float = RAG_THRESHOLD, max_results: int = 5):

    start_time = time()
    txn_id = txn.get("transaction_id") or txn.get("TransactionID") or txn.get("id")
    digest = []
    exceptions = []

    if not rag_results:
        exceptions.append({
            "TransactionID": txn_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Reason": "No RAG results found"
        })
        return digest, exceptions

    # Process top N results
    best_match = None
    best_score = 0
    for idx, result in enumerate(rag_results[:max_results]):
        # Extract numerical score from formatted match_score string
        score_str = result.get("match_score", "0%").replace("%", "")
        try:
            score = float(score_str)
        except:
            score = 0

        if score > best_score:
            best_score = score
            best_match = result

    if best_match and best_score >= threshold:
        # Digest entry
        digest.append({
            "TransactionID": txn_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Description": txn.get("description"),
            "Source": best_match["location"].get("pdf_name", ""),
            "Page": best_match["location"].get("page", ""),
            "EmailLink": f"https://mail.google.com/mail/u/0/#inbox/{best_match['location'].get('email_id', '')}",
            "EmailSender": best_match["location"].get("sender", ""),
            "EmailDate": best_match["location"].get("date", ""),
            "MatchScore": best_score,
            "MatchDetails": best_match["match_details"],
            "ContentPreview": best_match["content"][:300]
        })
    else:

        reason = f"Best RAG score {best_score:.2f}% below threshold {threshold}" if best_match else "No confident RAG match"
        exceptions.append({
            "TransactionID": txn_id,
            "TransactionDate": txn.get("date"),
            "Amount": txn.get("amount"),
            "VendorName": txn.get("vendor_name"),
            "Description": txn.get("description"),
            "BestScore": best_score,
            "Reason": reason
        })

    elapsed = time() - start_time
    print(f"Processed Transaction {txn_id} in {elapsed:.2f}s")
    return digest, exceptions


def hybrid_match_rag(transactions: List[Dict[str, Any]], emails: List[Dict[str, Any]], top_k_per_batch: int = 20, global_top_k: int = 3):

    all_digest = []
    all_exceptions = []


    batch_dirs = sorted([p for p in INDEX_ROOT.iterdir() if p.is_dir()])

    for txn in transactions:
        query_info = csv_row_to_enhanced_query(txn)
        print(query_info)
        rag_results_raw = global_search(query_info, batch_dirs, top_k=global_top_k, top_k_per_batch=top_k_per_batch, rerank=True)
        print(rag_results_raw)
        formatted_results = format_results(rag_results_raw)


        digest, exceptions = score_rag_transaction(txn, formatted_results)
        all_digest.extend(digest)
        all_exceptions.extend(exceptions)

    return all_digest, all_exceptions
