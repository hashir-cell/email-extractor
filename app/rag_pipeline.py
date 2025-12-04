import io
import json
import math
import re
from pathlib import Path
from typing import List, Dict, Any, Optional

import pdfplumber
from pdf2image import convert_from_path, convert_from_bytes
import pytesseract
from concurrent.futures import ThreadPoolExecutor

import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder

import faiss
from rank_bm25 import BM25Okapi
from langchain_text_splitters import RecursiveCharacterTextSplitter


BATCH_SIZE_EMAILS = 200
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBED_MODEL = "all-MiniLM-L6-v2"
RERANK_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"
TOP_K_PER_BATCH = 20  
GLOBAL_TOP_K = 3
INDEX_ROOT = Path("storage")
INDEX_ROOT.mkdir(exist_ok=True)

AMOUNT_TOLERANCE = 0.01  


def normalize_amount(amount_str: str) -> Optional[float]:
    if not amount_str:
        return None
    
    amount_str = str(amount_str)
    cleaned = re.sub(r'[$,\s€£¥₹]', '', amount_str)
    match = re.search(r'-?\d+\.?\d*', cleaned)
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    return None

def extract_amounts_from_text(text: str) -> List[float]:

    amounts = []

    patterns = [
        r'\$\s*[\d,]+\.?\d*', 
        r'[\d,]+\.?\d*\s*(?:USD|EUR|GBP|dollars?)',  
        r'(?:total|amount|sum|price|cost|invoice)[\s:]+\$?\s*[\d,]+\.?\d*',  
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.IGNORECASE)
        for match in matches:
            amt = normalize_amount(match.group())
            if amt:
                amounts.append(amt)
    
    return amounts

def amounts_match(target: float, candidates: List[float], tolerance: float = AMOUNT_TOLERANCE) -> bool:

    if not candidates:
        return False
    
    for candidate in candidates:
        diff = abs(target - candidate)
        threshold = target * tolerance
        if diff <= threshold:
            return True
    
    return False

def ocr_single_page(img):
    return pytesseract.image_to_string(img)

def extract_pages_from_pdf(pdf_source, dpi: int = 150) -> List[Dict[str, Any]]:
    results = []
    pages_need_ocr = []

    if isinstance(pdf_source, (bytes, bytearray)):
        pdf_fileobj = io.BytesIO(pdf_source)
        with pdfplumber.open(pdf_fileobj) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                tables = page.extract_tables() or []
                page_data = {"page_number": i, "text": "", "tables": tables, "method": ""}
                if text and len(text.strip()) > 50:
                    page_data["text"] = text.strip()
                    page_data["method"] = "text_extraction"
                else:
                    page_data["method"] = "ocr_pending"
                    pages_need_ocr.append(i)
                results.append(page_data)
    else:
        pdf_path = Path(pdf_source)
        with pdfplumber.open(str(pdf_path)) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                tables = page.extract_tables() or []
                page_data = {"page_number": i, "text": "", "tables": tables, "method": ""}
                if text and len(text.strip()) > 50:
                    page_data["text"] = text.strip()
                    page_data["method"] = "text_extraction"
                else:
                    page_data["method"] = "ocr_pending"
                    pages_need_ocr.append(i)
                results.append(page_data)

    if pages_need_ocr:
        groups = []
        current = [pages_need_ocr[0]]
        for p in pages_need_ocr[1:]:
            if p == current[-1] + 1:
                current.append(p)
            else:
                groups.append(current)
                current = [p]
        groups.append(current)

        all_images = []
        for g in groups:
            first_page = min(g)
            last_page = max(g)
            if isinstance(pdf_source, (bytes, bytearray)):
                images = convert_from_bytes(pdf_source, dpi=dpi, first_page=first_page, last_page=last_page)
            else:
                images = convert_from_path(str(pdf_source), dpi=dpi, first_page=first_page, last_page=last_page)
            all_images.extend(images)

        with ThreadPoolExecutor(max_workers=4) as ex:
            texts = list(ex.map(ocr_single_page, all_images))

        for idx, page_num in enumerate(pages_need_ocr):
            results[page_num - 1]["text"] = texts[idx].strip()
            results[page_num - 1]["method"] = "ocr"

    return results


text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""],
    is_separator_regex=False
)

def chunk_pages(pages: List[Dict[str, Any]], base_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:

    chunks = []
    for p in pages:
        page_num = p["page_number"]
        text = p.get("text", "") or ""
        tables = p.get("tables", []) or []
        method = p.get("method", "unknown")

        if text.strip():
            t_chunks = text_splitter.split_text(text)
            for t in t_chunks:
                if len(t.strip()) < 20:
                    continue
        
                amounts = extract_amounts_from_text(t)
                
                chunks.append({
                    "chunk_id": None,
                    "page": page_num,
                    "type": "text",
                    "content": t.strip(),
                    "extraction_method": method,
                    "char_count": len(t),
                    "amounts": amounts, 
                    "metadata": {**base_metadata}
                })

        for tidx, table in enumerate(tables):
            table_text_rows = [" | ".join([str(cell) if cell else "" for cell in row]) for row in table]
            table_text = "\n".join(table_text_rows)
            if len(table_text) == 0:
                continue

            amounts = extract_amounts_from_text(table_text)
            
            if len(table_text) > CHUNK_SIZE:
                t_chunks = text_splitter.split_text(table_text)
                for tc_idx, tc in enumerate(t_chunks):
                    if len(tc.strip()) < 20:
                        continue
                    tc_amounts = extract_amounts_from_text(tc)
                    chunks.append({
                        "chunk_id": None,
                        "page": page_num,
                        "type": "table",
                        "content": tc.strip(),
                        "extraction_method": "table",
                        "char_count": len(tc),
                        "amounts": tc_amounts,
                        "metadata": {**base_metadata, "table_index": tidx, "table_chunk": tc_idx}
                    })
            else:
                chunks.append({
                    "chunk_id": None,
                    "page": page_num,
                    "type": "table",
                    "content": table_text,
                    "extraction_method": "table",
                    "char_count": len(table_text),
                    "amounts": amounts,
                    "metadata": {**base_metadata, "table_index": tidx}
                })
    

    for i, c in enumerate(chunks):
        c["chunk_id"] = i
    return chunks


embed_model = SentenceTransformer(EMBED_MODEL)

def build_embeddings(chunks: List[Dict[str, Any]], batch_size: int = 64) -> np.ndarray:
    texts = [c["content"] for c in chunks]
    embeddings = embed_model.encode(texts, batch_size=batch_size, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)
    return embeddings

def build_faiss_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)
    return index

def build_bm25(chunks: List[Dict[str, Any]]):
    tokenized = [c["content"].lower().split() for c in chunks]
    bm25 = BM25Okapi(tokenized)
    return bm25, tokenized

def save_faiss(index: faiss.IndexFlat, path: Path):
    faiss.write_index(index, str(path))

def load_faiss(path: Path) -> faiss.IndexFlat:
    return faiss.read_index(str(path))

def save_json(obj: Any, path: Path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path: Path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def process_batch(batch_id: int, emails: List[Dict[str, Any]], storage_root: Path = INDEX_ROOT) -> Dict[str, Any]:
    
    batch_dir = storage_root / f"batch_{batch_id:04d}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    all_chunks: List[Dict[str, Any]] = []

    for email in emails:
        base_meta = {
            "email_id": email.get("email_id") or email.get("id"),
            "sender": email.get("from") or email.get("sender"),
            "date": email.get("date"),
        }
        
        # NEW: Add email metadata as searchable chunks
        email_text_parts = []
        if email.get("subject"):
            email_text_parts.append(f"Subject: {email['subject']}")
        if email.get("snippet") or email.get("body"):
            body = email.get("body") or email.get("snippet", "")
            email_text_parts.append(f"Body: {body}")
        
        if email_text_parts:
            email_content = "\n".join(email_text_parts)
            amounts = extract_amounts_from_text(email_content)
            
            all_chunks.append({
                "chunk_id": len(all_chunks),
                "page": 0,
                "type": "email_metadata",
                "content": email_content,
                "extraction_method": "email",
                "char_count": len(email_content),
                "amounts": amounts,
                "metadata": {**base_meta, "pdf_name": "Email Content"}
            })
        
        for att in email.get("attachments", []):
            pdf_bytes = att.get("bytes")
            if not pdf_bytes:
                continue

            try:
                pages = extract_pages_from_pdf(pdf_bytes)
            except Exception as e:
                print(f"Error extracting {att.get('filename')}: {e}")
                continue

            att_meta = {**base_meta, "pdf_name": att.get("filename")}
            page_chunks = chunk_pages(pages, att_meta)
            
            offset = len(all_chunks)
            for i, pc in enumerate(page_chunks):
                pc["chunk_id"] = offset + i
            
            all_chunks.extend(page_chunks)

    if len(all_chunks) == 0:
        save_json({"chunks_count": 0}, batch_dir / "manifest.json")
        return {"batch_id": batch_id, "chunks": 0}

    embeddings = build_embeddings(all_chunks)
    faiss_index = build_faiss_index(embeddings)
    save_faiss(faiss_index, batch_dir / "faiss.index")

    bm25_index, tokenized_texts = build_bm25(all_chunks)
    save_json([t for t in tokenized_texts], batch_dir / "bm25_tokenized.json")

    save_json(all_chunks, batch_dir / "chunks.json")

    manifest = {
        "batch_id": batch_id,
        "chunks": len(all_chunks),
        "faiss_path": str(batch_dir / "faiss.index"),
        "bm25_tokenized": str(batch_dir / "bm25_tokenized.json"),
        "chunks_path": str(batch_dir / "chunks.json")
    }
    save_json(manifest, batch_dir / "manifest.json")

    return manifest


def csv_row_to_enhanced_query(csv_row):

    structured = {}
    parts = []

    for key, value in csv_row.items():
        if value is None or value == "":
            continue

        key_lower = key.lower()

        if key_lower == "amount":
            try:
                structured["amount"] = float(value)
                parts.append(str(structured["amount"]))
            except:

                structured["amount"] = value
                parts.append(str(value))
        else:
            structured[key_lower] = value
            parts.append(str(value))

    return {
        "text_query": " ".join(parts),
        "structured_fields": structured
    }


def hybrid_retrieve_one_batch(query_info: Dict[str, Any], batch_obj: Dict[str, Any], top_k=TOP_K_PER_BATCH):

    faiss_idx = batch_obj["faiss"]
    bm25 = batch_obj["bm25"]
    chunks = batch_obj["chunks"]
    
    text_query = query_info['text_query']
    structured = query_info['structured_fields']
    target_amount = structured.get('amount')

    q_emb = embed_model.encode([text_query], convert_to_numpy=True, normalize_embeddings=True)
    try:
        distances, indices = faiss_idx.search(q_emb, min(top_k * 3, len(chunks)))
    except Exception:
        distances, indices = np.array([[]]), np.array([[]])

    dense_scores = {}
    for idx, score in zip(indices[0], distances[0]):
        dense_scores[int(idx)] = float(score)

    tok = text_query.lower().split()
    bm25_scores = bm25.get_scores(tok)


    candidate_idxs = set()
    candidate_idxs.update([int(i) for i in indices[0] if i is not None and i != -1])
    sparse_top = np.argsort(bm25_scores)[::-1][:top_k * 3]
    candidate_idxs.update([int(i) for i in sparse_top])

    candidates = []
    for i in candidate_idxs:
        chunk = chunks[i]
        
        dense_score = dense_scores.get(i, 0.0)
        sparse_score = float(bm25_scores[i])
        
        max_bm25 = max(bm25_scores) if max(bm25_scores) > 0 else 1.0
        sparse_score_norm = sparse_score / max_bm25
        
        base_score = 0.4 * dense_score + 0.6 * sparse_score_norm
        
        amount_boost = 0.0
        if target_amount and chunk.get('amounts'):
            if amounts_match(target_amount, chunk['amounts'], tolerance=AMOUNT_TOLERANCE):
                amount_boost = 2.0  
        
        vendor_boost = 0.0
        if structured.get('vendor'):
            vendor = structured['vendor'].lower()
            content_lower = chunk['content'].lower()
            if vendor in content_lower:
                vendor_boost = 0.5
        
        date_boost = 0.0
        if structured.get('date'):
            date_value = structured['date']
            date_str = str(date_value).lower()
            content_lower = chunk['content'].lower()

            if date_str in content_lower:
                date_boost = 0.3
        

        invoice_boost = 0.0
        if structured.get('invoice_number'):
            inv_num = structured['invoice_number'].lower()
            content_lower = chunk['content'].lower()
            if inv_num in content_lower:
                invoice_boost = 0.4
        

        final_score = base_score + amount_boost + vendor_boost + date_boost + invoice_boost
        
        candidates.append({
            "score": float(final_score),
            "chunk": chunk,
            "match_details": {
                "base_score": base_score,
                "amount_match": amount_boost > 0,
                "vendor_match": vendor_boost > 0,
                "date_match": date_boost > 0,
                "invoice_match": invoice_boost > 0
            }
        })
    
    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)[:top_k]
    return candidates


def load_batch_indices(batch_dir: Path):
    manifest = load_json(batch_dir / "manifest.json")
    if manifest.get("chunks", 0) == 0:
        return None
    faiss_idx = load_faiss(Path(manifest["faiss_path"]))
    tokenized_texts = load_json(Path(manifest["bm25_tokenized"]))
    bm25 = BM25Okapi(tokenized_texts)
    chunks = load_json(Path(manifest["chunks_path"]))
    return {"faiss": faiss_idx, "bm25": bm25, "chunks": chunks, "tokenized": tokenized_texts, "manifest": manifest}

def global_search(query_info: Dict[str, Any], batch_dirs: List[Path], top_k=GLOBAL_TOP_K, top_k_per_batch=TOP_K_PER_BATCH, rerank: bool = True):

    batch_objs = []
    for bd in batch_dirs:
        bo = load_batch_indices(bd)
        if bo:
            batch_objs.append(bo)

    all_candidates = []
    for bo in batch_objs:
        cand = hybrid_retrieve_one_batch(query_info, bo, top_k=top_k_per_batch)
        all_candidates.extend(cand)

    if not all_candidates:
        return []


    best_by_key = {}
    for c in all_candidates:
        chunk = c["chunk"]
        unique_key = json.dumps({
            "chunk_id": chunk.get("chunk_id"),
            "pdf": chunk.get("metadata", {}).get("pdf_name"),
            "email": chunk.get("metadata", {}).get("email_id")
        }, sort_keys=True)
        if unique_key not in best_by_key or c["score"] > best_by_key[unique_key]["score"]:
            best_by_key[unique_key] = c

    merged = list(best_by_key.values())
    merged = sorted(merged, key=lambda x: x["score"], reverse=True)


    if rerank and len(merged) > 0:
        texts = [m["chunk"]["content"] for m in merged]
        queries = [query_info['text_query']] * len(texts)
        reranker = CrossEncoder(RERANK_MODEL)
        rerank_scores = reranker.predict(list(zip(queries, texts)))
        for i, sc in enumerate(rerank_scores):
            amount_boost = 2.0 if merged[i]["match_details"]["amount_match"] else 0.0
            merged[i]["score_rerank"] = float(sc) + amount_boost
        merged = sorted(merged, key=lambda x: x.get("score_rerank", x["score"]), reverse=True)

    return merged[:top_k]

def format_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    formatted = []

    for r in results:
        chunk = r["chunk"]
        meta = chunk.get("metadata", {})
        md = r.get("match_details", {})

        base_score = md.get("base_score", 0.0)
        
        amount = md.get("amount_match", False)
        vendor = md.get("vendor_match", False)
        date = md.get("date_match", False)
        invoice = md.get("invoice_match", False)

        formatted_result = {
            "base_score": float(base_score),  
            "match_details": {
                "amount_matched": amount,
                "vendor_matched": vendor,
                "date_matched": date,
                "invoice_matched": invoice
            },
            "location": {
                "pdf_name": meta.get("pdf_name", "Unknown"),
                "page": chunk.get("page", "Unknown"),
                "email_id": meta.get("email_id", "Unknown"),
                "sender": meta.get("sender", "Unknown"),
                "date": meta.get("date", "Unknown")
            },
            "extracted_amounts": chunk.get("amounts", []),
            "content": chunk.get("content", ""),
            "type": chunk.get("type", "text"),
            "extraction_method": chunk.get("extraction_method", "unknown")
        }

        formatted.append(formatted_result)

    return formatted




def ingest_all_emails(email_inputs: List[Dict[str, Any]], batch_size: int = BATCH_SIZE_EMAILS) -> List[Dict[str, Any]]:
    manifests = []
    total = len(email_inputs)
    batches = math.ceil(total / batch_size)
    for i in range(batches):
        start = i * batch_size
        end = min(total, start + batch_size)
        batch_emails = email_inputs[start:end]
        manifest = process_batch(i + 1, batch_emails)
        manifests.append(manifest)
    return manifests


import shutil

def clean_storage():
    shutil.rmtree("storage", ignore_errors=True)

if __name__ == "__main__":
    import pickle

    with open("emails_with_bytes.pkl", "rb") as f:
        emails_for_rag = pickle.load(f)

    print(f"Loaded {len(emails_for_rag)} emails from pickle.")

    manifests = ingest_all_emails(emails_for_rag, batch_size=100)

    batch_dirs = sorted([p for p in INDEX_ROOT.iterdir() if p.is_dir()])


    csv_row = {
        'date': 'May 10 2023',
        'Vendor': 'amazon',
        'Description': 'ABC',
        'Amount': 361608.96,
        'Invoice number': None
    }
    

    query_info = csv_row_to_enhanced_query(csv_row)
    

    results = global_search(query_info, batch_dirs, top_k=3, top_k_per_batch=20, rerank=True)
    print(results)
    formatted_results = format_results(results)
    
    print("\n" + "="*80)
    print("ENHANCED SEARCH RESULTS")
    print("="*80)
    for i, result in enumerate(formatted_results, 1):
        print(f"\n--- RESULT {i} ---")
        print(f"Match Score: {result['match_score']}")
        print(f"Match Details:")
        print(f"  ✓ Amount Matched: {result['match_details']['amount_matched']}")
        print(f"  ✓ Vendor Matched: {result['match_details']['vendor_matched']}")
        print(f"  ✓ Date Matched: {result['match_details']['date_matched']}")
        print(f"  ✓ Invoice Matched: {result['match_details']['invoice_matched']}")
        print(f"Extracted Amounts: {result['extracted_amounts']}")
        print(f"Location:")
        print(f"  PDF: {result['location']['pdf_name']}")
        print(f"  Page: {result['location']['page']}")
        print(f"  Sender: {result['location']['sender']}")
        print(f"  Date: {result['location']['date']}")
        print(f"Content Preview: {result['content'][:300]}...")
        print("-" * 80)