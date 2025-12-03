const API_URL = "http://127.0.0.1:8000";

let SESSION_ID: string | null = null;

const getHeaders = () => {
  const current = localStorage.getItem("session_id");
  if (current && current !== SESSION_ID) {
    SESSION_ID = current;
  }
  if (!SESSION_ID) throw new Error("No session");
  return { "X-Session-ID": SESSION_ID };
};


const validateSession = async (sessionId: string): Promise<boolean> => {
  try {
    const res = await fetch(`${API_URL}/accounts`, {
      headers: { "X-Session-ID": sessionId }
    });
    if (!res.ok) return false;
    const data = await res.json();

    
    return Array.isArray(data.accounts);
  } catch {
    return false;
  }
};

export const ensureSession = async (): Promise<string> => {
  const current = localStorage.getItem("session_id");
  
  // If we have a session, validate it first
  if (current) {
    const isValid = await validateSession(current);
    if (isValid) {
      SESSION_ID = current;
      return current;
    } else {
      // Session is invalid, clear it
      localStorage.removeItem("session_id");
      SESSION_ID = null;
    }
  }

  // Create new session
  const res = await fetch(`${API_URL}/session`, { method: "POST" });
  if (!res.ok) throw new Error("Failed to create session");
  const data = await res.json();
  SESSION_ID = data.session_id;
  localStorage.setItem("session_id", SESSION_ID as string);
  return SESSION_ID as string;
};

export const loginProvider = async () => {
  await ensureSession();
  const res = await fetch(`${API_URL}/login/gmail`, { headers: getHeaders() });
  if (!res.ok) throw new Error("Login failed");
  const { auth_url } = await res.json();
  return auth_url;
};

export const getAccounts = async (): Promise<string[]> => {
  await ensureSession();
  const res = await fetch(`${API_URL}/accounts`, { headers: getHeaders() });
  if (!res.ok) return [];
  const data = await res.json();
  return data.accounts || [];
};

export const disconnectAccount = async (account: string) => {
  await ensureSession();
  
  // Backend expects: DELETE /accounts?account=email@gmail.com (gmail)
  const res = await fetch(`${API_URL}/accounts?account=${encodeURIComponent(account)}`, {
    method: "DELETE",
    headers: getHeaders(),
  });
  
  if (!res.ok) {
    const errorText = await res.text().catch(() => "Unknown error");
    console.error("Disconnect failed:", res.status, errorText);
    throw new Error(`Failed to disconnect account: ${res.status}`);
  }
  
  // Check if this was the last account
  const remainingAccounts = await getAccounts();
  if (remainingAccounts.length === 0) {
    // Clear session when last account is disconnected
    clearSession();
  }
  
  return remainingAccounts;
};

export const processCsv = async (file: File, accounts: string[]) => {
  await ensureSession();
  const form = new FormData();
  form.append("file", file);
  
  // Backend expects: accounts as multiple Form fields
  accounts.forEach(acc => form.append("accounts", acc));
  
  const res = await fetch(`${API_URL}/process`, {
    method: "POST",
    headers: getHeaders(),
    body: form,
  });
  
  if (!res.ok) {
    const errorText = await res.text().catch(() => "Processing failed");
    throw new Error(errorText);
  }
  
  return res.json();
};

// Helper to clear session
export const clearSession = () => {
  localStorage.removeItem("session_id");
  SESSION_ID = null;
};

// Helper to check if session exists
export const hasSession = (): boolean => {
  return !!localStorage.getItem("session_id");
};