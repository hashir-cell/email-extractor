// src/App.tsx
import { useState } from "react";
import { LoginButton } from "./components/LoginButton";
import { AccountList } from "./components/AccountList";
import { CsvUpload } from "./components/CsvUpload";
import "bootstrap/dist/css/bootstrap.min.css";

function App() {
  const [selectedAccounts, setSelectedAccounts] = useState<string[]>([]);
  const [connectedAccounts, setConnectedAccounts] = useState<string[]>([]);

  const handleLoginSuccess = () => {
    // Refresh account list after login
    window.location.reload();
    // Or better: just trigger a refresh in AccountList via state
    // But reload is simplest and 100% reliable
  };

  const handleAccountsChange = (accounts: string[]) => {
    setConnectedAccounts(accounts);
    
    // Clean up selected accounts that no longer exist
    setSelectedAccounts(prev => 
      prev.filter(acc => accounts.includes(acc))
    );
  };

  return (
    <div className="container py-5">
      {/* Header */}
      <div className="text-center mb-5">
        <h1 className="display-5 fw-bold text-primary">Financial Analyst</h1>
        <p className="lead text-muted">
          Automatically match bank transactions with receipts from your Gmail
        </p>
      </div>

      {/* Login Section */}
      <div className="row justify-content-center mb-5">
        <div className="col-md-6 text-center">
          <LoginButton onSuccess={handleLoginSuccess} />
          <p className="text-muted small mt-3">
            Click to connect your Gmail account
          </p>
        </div>
      </div>

      {/* Connected Accounts */}
      <div className="row justify-content-center mb-4">
        <div className="col-lg-8">
          <AccountList
            selected={selectedAccounts}
            onChange={setSelectedAccounts}
            onAccountsChange={handleAccountsChange}
          />
        </div>
      </div>

      {/* CSV Upload */}
      {selectedAccounts.length > 0 && (
        <div className="row justify-content-center">
          <div className="col-lg-8">
            <CsvUpload selectedAccounts={selectedAccounts} />
          </div>
        </div>
      )}

      {/* How It Works Section */}
      <div className="row justify-content-center mt-5">
        <div className="col-lg-8">
          <div className="card border-0 shadow-sm">
            <div className="card-body">
              <h5 className="card-title mb-4 text-center">How It Works</h5>
              <div className="row g-4">
                <div className="col-md-4">
                  <div className="text-center">
                    <div className="bg-primary text-white rounded-circle d-inline-flex align-items-center justify-content-center mb-3" 
                         style={{width: "48px", height: "48px"}}>
                      <strong className="fs-5">1</strong>
                    </div>
                    <h6 className="fw-semibold mb-2">Connect Gmail</h6>
                    <p className="text-muted small mb-0">
                      Login and select the accounts containing your receipts
                    </p>
                  </div>
                </div>
                
                <div className="col-md-4">
                  <div className="text-center">
                    <div className="bg-primary text-white rounded-circle d-inline-flex align-items-center justify-content-center mb-3" 
                         style={{width: "48px", height: "48px"}}>
                      <strong className="fs-5">2</strong>
                    </div>
                    <h6 className="fw-semibold mb-2">Upload CSV</h6>
                    <p className="text-muted small mb-0">
                      Upload your bank statement in CSV format
                    </p>
                  </div>
                </div>
                
                <div className="col-md-4">
                  <div className="text-center">
                    <div className="bg-primary text-white rounded-circle d-inline-flex align-items-center justify-content-center mb-3" 
                         style={{width: "48px", height: "48px"}}>
                      <strong className="fs-5">3</strong>
                    </div>
                    <h6 className="fw-semibold mb-2">Download Results</h6>
                    <p className="text-muted small mb-0">
                      Get matched transactions and exceptions reports
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="text-center mt-5 text-muted small">
        <p>
          {selectedAccounts.length > 0
            ? `Ready to scan ${selectedAccounts.length} Gmail account(s)`
            : "Connect a Gmail account to get started"}
        </p>
      </div>
    </div>
  );
}

export default App;