import os
import json
import re
import logging
from time import time
import google.generativeai as genai
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))


def score_match_with_gemini(transaction: dict, email_content: dict) -> dict:
    start_time = time()
    
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    attachments_text = ', '.join([a.get('filename', '') for a in email_content.get('attachments', [])])
    body_preview = email_content.get('body', 'N/A')[:2000]
    
    prompt = f"""You are a financial transaction matching expert. Analyze if the following email matches the transaction record.

TRANSACTION RECORD:
- Transaction Number: {transaction.get('transaction_id', 'N/A')}
- Date: {transaction.get('date', 'N/A')}
- Amount: ${transaction.get('amount', 'N/A')}
- Vendor: {transaction.get('vendor_name', 'N/A')}
- Description: {transaction.get('description', 'N/A')}

EMAIL DATA:
- From: {email_content.get('from', 'N/A')}
- Subject: {email_content.get('subject', 'N/A')}
- Date: {email_content.get('date', 'N/A')}
- Body: {body_preview}
- Attachments: {attachments_text if attachments_text else 'None'}

TASK:
1. Determine if this email is related to the transaction
2. Check for matching: transaction number, amount, vendor name, date proximity, description
3. Assign a match score from 0-100:
   - 90-100: Definite match (all key fields match)
   - 70-89: Strong match (most fields match)
   - 50-69: Probable match (some fields match)
   - 30-49: Weak match (minimal correlation)
   - 0-29: No match

RESPONSE FORMAT (JSON only, no markdown):
{{
  "score": <number 0-100>,
  "reason": "<brief explanation>",
  "matched_fields": ["field1", "field2"],
  "confidence": "<high/medium/low>"
}}"""

    try:
        response = model.generate_content(prompt)
        response_text = response.text.strip()
        
        if response_text.startswith("```json"):
            response_text = response_text.replace("```json", "").replace("```", "").strip()
        
        json_match = json.loads(response_text)
        
        result = {
            "score": int(json_match.get("score", 0)),
            "reason": json_match.get("reason", ""),
            "matched_fields": json_match.get("matched_fields", []),
            "confidence": json_match.get("confidence", "low")
        }
        
        elapsed = time() - start_time
        logger.debug(f"Gemini API call successful ({elapsed:.2f}s) - Score: {result['score']}")
        
        return result
    
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error, attempting regex fallback: {str(e)}")
        try:
            score_match = re.search(r'"?score"?\s*:\s*(\d+)', response_text)
            reason_match = re.search(r'"?reason"?\s*:\s*"([^"]+)"', response_text)
            
            return {
                "score": int(score_match.group(1)) if score_match else 0,
                "reason": reason_match.group(1) if reason_match else "Parse error",
                "matched_fields": [],
                "confidence": "low"
            }
        except Exception as e2:
            logger.error(f"Regex fallback failed: {str(e2)}")
            return {
                "score": 0,
                "reason": f"Failed to parse Gemini response: {str(e)}",
                "matched_fields": [],
                "confidence": "low"
            }
    
    except Exception as e:
        elapsed = time() - start_time
        logger.error(f"Gemini API error ({elapsed:.2f}s): {str(e)}")
        return {
            "score": 0,
            "reason": f"API Error: {str(e)}",
            "matched_fields": [],
            "confidence": "low"
        }


def batch_score_with_gemini(transaction: dict, emails: list, max_batch: int = 10) -> list:
    logger.info(f"Batch scoring {min(len(emails), max_batch)} emails")
    
    results = []
    
    for idx, email in enumerate(emails[:max_batch]):
        logger.info(f"  Batch {idx+1}/{min(len(emails), max_batch)}")
        result = score_match_with_gemini(transaction, email)
        result['email_id'] = email.get('id')
        results.append(result)
    
    results.sort(key=lambda x: x['score'], reverse=True)
    
    logger.info(f"Batch complete - Best score: {results[0]['score'] if results else 0}")
    
    return results