import pandas as pd
from typing import List, Set, Dict, Any, Tuple
from datetime import datetime
import google.generativeai as genai
import json
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from typing import List
from dotenv import load_dotenv
load_dotenv()

class Transaction(BaseModel):
    transaction_id: str
    date:datetime = None
    amount: float = None
    vendor: Optional[str] = None
    vendor_domain: Optional[str] = None
    description: Optional[str] = None


load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

DATE_FORMATS = [
    "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y", "%Y/%m/%d",
    "%b %d, %Y", "%d %b %Y", "%m/%d/%y", "%d/%m/%y"
]

class TransactionCleaner:
    def __init__(self):
        pass

    def _prepare_column_analysis(self,df:pd.DataFrame,max_sample_rows:int=3)->List[Dict[str,Any]]:
        
        column_info=[]

        for col_idx,col_name in enumerate(df.columns):

            sample_values=[]
            for row_idx in range(min(max_sample_rows,len(df))):
                value=df.iloc[row_idx,col_idx]
                sample_values.append(str(value) if pd.notna(value) else "")
            
            column_info.append({
                "column_idx":col_idx,
                "column_name":col_name,
                "sample_values":sample_values
            })

        return column_info

    def _map_column_with_llm(self,column_info:List[Dict[str,Any]])->Dict[str,int]:

        if not column_info:
            return {}
        
        column_desc=[]

        for col in column_info:
            sample_str=",".join([f"{v}" for v in col["sample_values"]])
            column_desc.append(f'Column {col["column_idx"]} named "{col["column_name"]}" has sample values: {sample_str}')

        prompt = f"""
You are analyzing the columns of an EXPENSE or BANK STATEMENT CSV file.

Below is the detected column summary (with sample values):

{chr(10).join(column_desc)}

Your task:
Identify which column INDEX (0, 1, 2, etc.) corresponds to each of the following fields:

- transaction_id: Transaction, reference, or invoice numbers (usually alphanumeric identifiers)
- date: Transaction date (any format like DD/MM/YYYY, YYYY-MM-DD, etc.)
- amount: Transaction amount (positive or negative values, typically monetary)
- vendor_name (optional): Merchant OR vendor (e.g., Walmart, Shell, Uber)
- vendor_domain (optional): Website or domain name of the vendor if present (e.g., amazon.com)
- description: Text field that explains or describes the transaction (often the memo, note, or purpose of payment)

---

### CRITICAL INSTRUCTIONS
1. Always return the **column INDEX**, not the column name.
2. When multiple columns have similar names, rely on **sample values** to determine the correct one.
3. For `amount`: choose the column showing **actual money movement** (not balances or totals).
4. For `date`: ensure values match a valid date format.
5. For `transaction_id`: look for alphanumeric strings or reference codes uniquely identifying each transaction.
6. For `vendor_name` / `vendor_domain`: infer only if clearly indicated by sample values.
7. For `description`: pick the column that contains natural-language text describing the transaction.
8. If uncertain about any field, set it to `null`.
9. Return **only valid JSON** — no markdown, no commentary.

---

### OUTPUT FORMAT
Return a JSON object in the following structure:

{{
  "transaction_id": <index or null>,
  "date": <index or null>,
  "amount": <index or null>,
  "vendor_name": <index or null>,
  "vendor_domain": <index or null>,
  "description": <index or null>
}}

Example:
{{
  "transaction_id": 0,
  "date": 1,
  "amount": 3,
  "vendor_name": 2,
  "vendor_domain": null,
  "description": 4
}}
"""
        try:
            model=genai.GenerativeModel("gemini-2.0-flash-exp")
            response=model.generate_content(prompt)
            response_json=response.text.strip()

            if response_json.startswith("```json"):
                lines=response_json.split("\n")
                response_json="\n".join(lines[1:-1]) if len(lines)>2 else response_json
                response_json=response_json.replace("```json","").replace("```","").strip()

            mapping=json.loads(response_json)

            max_idx=len(column_info)-1
            for key,value in mapping.items():
                if value is not None and (not isinstance(value,int) or  value<0 or value>max_idx):
                    print(f"Warning: Invalid index {value} for {key}, setting to None")
                    mapping[key] = None
            
            print(f"LLM mapped: {mapping}")
            return mapping
            
        except Exception as e:
            print(f"LLM error: {e}.")

    
    def parse_csv(self,file)->List[Transaction]:

        if hasattr(file, "name"):
            if not file.name.endswith(".csv"):
                raise ValueError("Only CSV Supported")
        
        try:
            df=pd.read_csv(file,dtype=str,keep_default_na=False)
            if df.empty:
                return []
        except Exception as e:
            raise ValueError("Error reading CSV")
        
        column_info = self._prepare_column_analysis(df)
        col_map = self._map_column_with_llm(column_info)

        mapped_columns={k:v for k,v in col_map.items() if v is not None}
        if not mapped_columns:
            print("No columns mapped by LLM.")
            return pd.DataFrame()
        
        subset = df.iloc[:, list(mapped_columns.values())].copy()
        subset.columns = list(mapped_columns.keys())


        base_dir = os.getcwd()
        out_dir = os.path.join(base_dir, 'out')
        os.makedirs(out_dir, exist_ok=True)

        export_path = os.path.join(out_dir, 'mapped.csv')
        subset.to_csv(export_path, index=False)

        print(f"✅ Exported mapped columns only to: {export_path}")
        return subset
    
parser = TransactionCleaner()



if __name__ == "__main__":
    parser = TransactionCleaner()
    transactions = parser.parse_csv("app\data\Loan Payments CSV - BMO $3,787.47.csv")

        



            
