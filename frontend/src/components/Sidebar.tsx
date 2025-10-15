import React, { useState } from "react";
import { FaTachometerAlt } from "react-icons/fa";

const sidebarWidth = 64;
const expandedWidth = 220;

export default function Sidebar({ active = "dashboard" }: { active?: string }) {
  const [expanded, setExpanded] = useState(false);
  const [hovered, setHovered] = useState<string | null>(null);

  return (
    <div
      className="sidebar frosted"
      onMouseEnter={() => setExpanded(true)}
      onMouseLeave={() => setExpanded(false)}
      style={{
        width: expanded ? expandedWidth : sidebarWidth,
        transition: "width 0.2s cubic-bezier(.4,2,.6,1)",
        minHeight: "100vh",
        color: "var(--color-white)",
        position: "fixed",
        left: 0,
        top: 0,
        zIndex: 10000,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        paddingTop: 32,
        background: "rgba(0,0,20,0.7)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          gap: 12,
          width: expanded ? "100%" : sidebarWidth,
          padding: expanded ? "0 24px" : "0",
          marginBottom: 32,
        }}
      ></div>
      <div
        className="sidebar-link"
        style={{
          display: "flex",
          flexDirection: expanded ? "row" : "column",
          alignItems: "center",
          justifyContent: "center",
          width: expanded ? "100%" : sidebarWidth,
          height: 48,
          cursor: "pointer",
          color: "var(--color-white)",
          background:
            hovered === "dashboard" ? "rgba(0,0,128,0.12)" : "transparent",
          borderRadius: 8,
          marginBottom: 8,
          transition: "background 0.2s, color 0.2s",
        }}
        onMouseEnter={() => setHovered("dashboard")}
        onMouseLeave={() => setHovered(null)}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            width: 32,
            height: 32,
          }}
        >
          <FaTachometerAlt size={24} />
        </div>
        {expanded && (
          <span style={{ fontSize: 16, marginLeft: 8 }}>Dashboard</span>
        )}
      </div>
      {/* Add more links here as you add more pages */}
    </div>
  );
}
