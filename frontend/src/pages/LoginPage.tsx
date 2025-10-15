import React from "react";

export default function LoginPage({ onLogin }: { onLogin: () => void }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        marginTop: 100,
      }}
    >
      <h2>Login</h2>
      <button onClick={onLogin} style={{ padding: "10px 30px", fontSize: 18 }}>
        Login
      </button>
    </div>
  );
}
