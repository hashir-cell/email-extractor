// src/components/LoginButton.tsx
import { loginProvider } from "../api";

export const LoginButton = ({ onSuccess }: { onSuccess: () => void }) => {
  const handleLogin = async () => {
    try {
      // Step 1: Get the Google auth URL
      const authUrl = await loginProvider();

      // Step 2: Open popup with a clean blank page (no document.write!)
      const popup = window.open(
        "", 
        "gmail-login",
        "width=500,height=650,scrollbars=yes,resizable=yes"
      );

      if (!popup) {
        alert("Please allow popups for this site");
        return;
      }

      // Step 3: Immediately redirect — this is safe
      popup.location.href = authUrl;

      // Optional: Show a nice loading page BEFORE redirect (still safe)
      // We do this by opening a data URL first
      const loadingHtml = `
        <!DOCTYPE html>
        <html>
          <head><title>Connecting to Gmail...</title></head>
          <body style="margin:0;font-family:system-ui;background:#f8f9fa;">
            <div style="text-align:center;padding:80px 20px;">
              <h2>Connecting to Gmail</h2>
              <p>Please wait while we redirect you to Google...</p>
              <div style="margin:40px auto;width:40px;height:40px;border:5px solid #eee;border-top:5px solid #dc3545;border-radius:50%;animation:s 1s linear infinite"></div>
            </div>
            <style>@keyframes s{to{transform:rotate(360deg)}}</style>
          </body>
        </html>
      `;

      // Open loading page first, then redirect
      popup.location.href = "data:text/html;base64," + btoa(loadingHtml);
      setTimeout(() => {
        popup.location.href = authUrl;
      }, 100);

      // Step 4: Listen for success message from backend
      const messageHandler = (e: MessageEvent) => {
        if (e.data?.email && e.data?.provider === "gmail") {
          window.removeEventListener("message", messageHandler);
          popup.close();
          onSuccess();
        }
      };

      window.addEventListener("message", messageHandler);

      // Cleanup if user closes popup
      const checkClosed = setInterval(() => {
        if (popup.closed) {
          clearInterval(checkClosed);
          window.removeEventListener("message", messageHandler);
        }
      }, 500);

    } catch (err: any) {
      alert("Login failed: " + err.message);
      console.error(err);
    }
  };

  return (
    <button
      onClick={handleLogin}
      className="btn btn-danger btn-lg px-5 shadow-sm"
      style={{ fontWeight: 600 }}
    >
      Login with Gmail
    </button>
  );
};