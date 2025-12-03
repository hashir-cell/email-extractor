// src/components/CsvUpload.tsx
import { useState } from "react";
import { processCsv } from "../api";

interface Result {
  digest_csv: string;
  digest_filename: string;
  exceptions_csv: string;
  exceptions_filename: string;
}

interface Props {
  selectedAccounts: string[];
}

export const CsvUpload = ({ selectedAccounts }: Props) => {
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [progress, setProgress] = useState("");
  const [result, setResult] = useState<Result | null>(null);

  const handleUpload = async () => {
    if (!file) return alert("Please select a CSV file");
    if (selectedAccounts.length === 0) return alert("Please select at least one Gmail account");

    setUploading(true);
    setProgress("Uploading CSV...");

    try {
      setProgress("Scanning your Gmail accounts for receipts...");
      const data = await processCsv(file, selectedAccounts);

      setResult({
        digest_csv: data.digest_csv,
        digest_filename: data.digest_filename || "ExpenseDigest.csv",
        exceptions_csv: data.exceptions_csv,
        exceptions_filename: data.exceptions_filename || "UnmatchedTransactions.csv",
      });

      setProgress("Ready! Click below to download your CSVs");
    } catch (err: any) {
      alert("Processing failed: " + err.message);
      console.error(err);
      setResult(null);
    } finally {
      setUploading(false);
      setTimeout(() => setProgress(""), 8000);
    }

    // Clear file input after successful processing
    setFile(null);
    const input = document.getElementById("csv-input") as HTMLInputElement;
    if (input) input.value = "";
  };

  const downloadFile = (base64: string, filename: string) => {
    const link = document.createElement("a");
    link.href = `data:text/csv;base64,${base64}`;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleProcessAnother = () => {
    // Only reset the results view and file input
    // Keep the session and selected accounts intact
    setResult(null);
    setProgress("");
    setFile(null);
    const input = document.getElementById("csv-input") as HTMLInputElement;
    if (input) input.value = "";
  };

  const canUpload = !!file && selectedAccounts.length > 0 && !uploading;

  return (
    <div className="card border-0 shadow-sm">
      <div className="card-body">
        <h5 className="card-title mb-3">Upload Bank Statement CSV</h5>

        {/* Only show upload UI when not showing results */}
        {!result && (
          <>
            <input
              id="csv-input"
              type="file"
              accept=".csv,text/csv"
              className="form-control form-control-lg mb-3"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              disabled={uploading}
            />
            {file && <small className="text-success d-block mb-3">Selected: {file.name}</small>}

            <button
              className={`btn btn-lg w-100 ${canUpload ? "btn-success" : "btn-secondary"} mb-4`}
              onClick={handleUpload}
              disabled={!canUpload}
            >
              {uploading ? (
                <>
                  <span className="spinner-border spinner-border-sm me-2" />
                  Processing...
                </>
              ) : (
                "Process Transactions"
              )}
            </button>

            {/* Progress */}
            {progress && (
              <div className="alert alert-info py-2 text-center small mb-4">
                {progress}
              </div>
            )}
          </>
        )}

        {/* Download Buttons - Only show when results are ready */}
        {result && (
          <div className="text-center">
            <h5 className="text-success mb-4">
              <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" fill="currentColor" className="bi bi-check-circle-fill me-2" viewBox="0 0 16 16">
                <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0m-3.97-3.03a.75.75 0 0 0-1.08.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-.01-1.05z"/>
              </svg>
              Processing Complete!
            </h5>

            <div className="d-grid gap-3">
              <button
                onClick={() => downloadFile(result.digest_csv, result.digest_filename)}
                className="btn btn-outline-success btn-lg"
              >
                📊 Download Expense Digest
                <br />
                <small className="text-muted">Matched transactions with receipts</small>
              </button>

              <button
                onClick={() => downloadFile(result.exceptions_csv, result.exceptions_filename)}
                className="btn btn-outline-warning btn-lg"
              >
                ⚠️ Download Unmatched Transactions
                <br />
                <small className="text-muted">Missing receipts or unknown entries</small>
              </button>
            </div>

            <button
              onClick={handleProcessAnother}
              className="btn btn-primary mt-4 px-4"
            >
              Process Another File
            </button>
            
            <p className="text-muted small mt-3 mb-0">
              Your Gmail accounts remain connected
            </p>
          </div>
        )}

        {/* Selected Accounts - Always visible */}
        {!result && (
          <div className="mt-4 small text-muted text-center">
            <strong>Selected Accounts:</strong>{" "}
            {selectedAccounts.length > 0 ? selectedAccounts.join(", ") : "None selected"}
          </div>
        )}
      </div>
    </div>
  );
};