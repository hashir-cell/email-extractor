import pandas as pd
from datetime import datetime
from typing import List, Dict
import re

import pandas as pd
from datetime import datetime
from typing import List, Dict
import re
from semantic_parsing import parser


def domain_to_vendor(domain: str) -> str:

    if not isinstance(domain, str) or not domain.strip():
        return ""
    domain = domain.lower().strip()

    parts = domain.split(".")
    if len(parts) >= 2:
        vendor_part = parts[-2] 
    else:
        vendor_part = parts[0]
    return vendor_part.capitalize()


def clean_transactions(df:pd.DataFrame) -> List[Dict]:
    df = df.copy()


    df["Vendor"] = df["Vendor"].fillna("").str.lower().str.strip() if "Vendor" in df.columns else ""
    df["vendor_name"] = df["Vendor"].apply(domain_to_vendor) if "Vendor" in df.columns else ""
    df["description"] = df["description"].fillna("").str.strip() if "description" in df.columns else ""
    df["transaction_id"] = df["transaction_id"].astype(str).str.strip() if "transaction_id"  in df.columns else "" 
    df["amount"] = df["amount"].apply(lambda x: abs(float(str(x).replace(",", "").strip())))
    df["date"] = pd.to_datetime(df["date"], errors="coerce")


    cleaned = df.to_dict(orient="records")
    print(f"✅ {len(cleaned)} cleaned transactions ready for matching.")
    return cleaned

