import React from "react";
import { FaCog } from "react-icons/fa";

export default function TopBar() {
  return (
    <div
      className="frosted"
      style={{
        width: "100%",
        height: 56,
        position: "fixed",
        top: 0,
        left: 0,
        zIndex: 9999,
        borderBottom: "2px solid var(--color-orange)",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        padding: "0 32px",
        boxShadow: "0 2px 12px 0 rgba(0,0,0,0.12)",
        overflow: "hidden",
        background: "rgba(255,255,255,0.9)",
      }}
    >
      {/* Banner background */}
      <img
        src="/assets/FD-Logo.png"
        alt="Banner"
        style={{
          position: "absolute",
          left: 0,
          top: -2,
          width: "100%",
          height: "100%",
          objectFit: "contain",
          objectPosition: "center",
          zIndex: 0,
          opacity: 1,
          pointerEvents: "none",
        }}
      />
      {/* Left: Logo or Title */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 16,
          zIndex: 1,
          marginLeft: "20px",
        }}
      >
        <span
          style={{
            color: "var(--color-orange)",
            fontWeight: 700,
            fontSize: 24,
            letterSpacing: 1,
            textShadow: "0 2px 8px rgba(0,0,0,0.8)",
          }}
        ></span>
      </div>
      {/* Right: Settings Icon */}
      <div style={{ zIndex: 1 }}>
        <span
          style={{
            cursor: "pointer",
            filter: "drop-shadow(0 0 4px #007BFF)",
            transition: "color 0.2s",
            display: "flex",
            alignItems: "center",
          }}
          title="Settings"
        >
          <FaCog size={26} color="var(--color-white)" />
        </span>
      </div>
    </div>
  );
}
