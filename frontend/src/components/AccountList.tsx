// src/components/AccountList.tsx
import { getAccounts, disconnectAccount } from "../api";
import { useState, useEffect } from "react";

interface Props {
  selected: string[];
  onChange: (selected: string[]) => void;
  onAccountsChange?: (accounts: string[]) => void; // Callback to notify parent
}

export const AccountList = ({ selected, onChange, onAccountsChange }: Props) => {
  const [accounts, setAccounts] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const loadAccounts = async (isRefresh = false) => {
    try {
      if (isRefresh) {
        setRefreshing(true);
      } else {
        setLoading(true);
      }
      
      const list = await getAccounts();
      setAccounts(list);
      
      // Notify parent component about account changes
      if (onAccountsChange) {
        onAccountsChange(list);
      }
      
      // Clean up selected accounts that no longer exist
      const validSelected = selected.filter(acc => list.includes(acc));
      if (validSelected.length !== selected.length) {
        onChange(validSelected);
      }
    } catch (err) {
      console.error("Failed to load accounts:", err);
      setAccounts([]);
      if (onAccountsChange) {
        onAccountsChange([]);
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadAccounts();
  }, []);

  const toggle = (acc: string) => {
    const newSelected = selected.includes(acc)
      ? selected.filter(a => a !== acc)
      : [...selected, acc];
    onChange(newSelected);
  };

  const handleDisconnect = async (acc: string) => {
    if (!confirm(`Disconnect ${acc}?`)) return;

    try {
      const remainingAccounts = await disconnectAccount(acc);
      setAccounts(remainingAccounts);
      onChange(selected.filter(a => a !== acc));
      
      // Notify parent about account changes
      if (onAccountsChange) {
        onAccountsChange(remainingAccounts);
      }
    } catch (err) {
      alert("Failed to disconnect account");
      console.error(err);
    }
  };

  const handleRefresh = () => {
    loadAccounts(true);
  };

  return (
    <div className="card border-0 shadow-sm">
      <div className="card-body">
        <div className="d-flex justify-content-between align-items-center mb-3">
          <h5 className="mb-0 fw-semibold">Connected Gmail Accounts</h5>
          <button
            className="btn btn-sm btn-outline-secondary"
            onClick={handleRefresh}
            disabled={loading || refreshing}
          >
            {refreshing ? (
              <>
                <span className="spinner-border spinner-border-sm me-1" />
                Refreshing...
              </>
            ) : (
              "Refresh"
            )}
          </button>
        </div>

        {loading ? (
          <p className="text-muted small">Loading accounts...</p>
        ) : accounts.length === 0 ? (
          <p className="text-muted mb-0">
            No Gmail accounts connected yet. Click "Login with Gmail" to add one.
          </p>
        ) : (
          <div className="list-group list-group-flush">
            {accounts.map((acc) => (
              <div
                key={acc}
                className="list-group-item px-0 py-2 d-flex align-items-center justify-content-between"
              >
                <div className="form-check">
                  <input
                    className="form-check-input"
                    type="checkbox"
                    id={`chk-${acc}`}
                    checked={selected.includes(acc)}
                    onChange={() => toggle(acc)}
                  />
                  <label className="form-check-label fw-medium" htmlFor={`chk-${acc}`}>
                    {acc}
                  </label>
                </div>
                <button
                  className="btn btn-sm btn-outline-danger"
                  onClick={() => handleDisconnect(acc)}
                >
                  Disconnect
                </button>
              </div>
            ))}
          </div>
        )}

        {accounts.length > 0 && (
          <small className="text-muted d-block mt-3">
            {selected.length} of {accounts.length} selected
          </small>
        )}
      </div>
    </div>
  );
};