from datetime import datetime, timedelta
import streamlit as st
import pandas as pd
from io import StringIO
from gmail_utils import fetch_recent_emails
from matching_engine import hybrid_match
from semantic_parsing import parser
from transaction_cleaner import clean_transactions

st.title("Financial Analyst")

uploaded_file=st.file_uploader("Upload your CSV Here",type="csv")

if uploaded_file is not None:

        results=parser.parse_csv(uploaded_file)
        transactions=clean_transactions(results)
        
        all_emails = []
        for txn in transactions:
          txn_date = txn["date"]
          
          if isinstance(txn_date, str):
            txn_date = pd.to_datetime(txn_date, errors="coerce")
            
          start_date = (txn_date - timedelta(days=4)).strftime("%Y/%m/%d")
          end_date = (txn_date + timedelta(days=4)).strftime("%Y/%m/%d")
            
          emails = fetch_recent_emails(start_date, end_date)
          print(emails)
          all_emails.extend(emails)
        
        print("🧠 Running hybrid rule-based + LLM matching...")
        digest, exceptions = hybrid_match(transactions, all_emails)

        digest_csv=digest.to_csv(index=False).encode("utf-8") if isinstance(digest,pd.DataFrame) else pd.DataFrame(digest).to_csv(index=False).encode("utf-8")

        exceptions_csv = exceptions.to_csv(index=False).encode("utf-8") if isinstance(exceptions, pd.DataFrame) else pd.DataFrame(exceptions).to_csv(index=False).encode("utf-8")
        
        st.download_button(
    label="Download Expense Digest",
    data=digest_csv,
    file_name=f"ExpenseDigest_{datetime.now():%Y-%m-%d}.csv",
    mime="text/csv"
)
        st.download_button(
    label="Download Exceptions Register",
    data=exceptions_csv,
    file_name=f"ExceptionsRegister_{datetime.now():%Y-%m-%d}.csv",
    mime="text/csv"
)


