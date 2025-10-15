import React, { useState } from "react";
import Sidebar from "../components/Sidebar";
import TopBar from "../components/TopBar";
import JobPostingSummary from "./JobPostingSummary";
import { FaChevronDown, FaChevronRight } from "react-icons/fa";

// Collapsible Section Component
interface CollapsibleSectionProps {
  title: string;
  isExpanded: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}

const CollapsibleSection: React.FC<CollapsibleSectionProps> = ({
  title,
  isExpanded,
  onToggle,
  children,
}) => {
  return (
    <div
      style={{
        border: "2px solid var(--color-blue)",
        borderRadius: "12px",
        overflow: "hidden",
        background: "rgba(0, 0, 0, 0.4)",
        boxShadow: "0 4px 12px rgba(0, 0, 0, 0.3)",
      }}
    >
      <button
        onClick={onToggle}
        style={{
          width: "100%",
          padding: "20px 24px",
          background:
            "linear-gradient(135deg, var(--color-navy-end), var(--color-blue))",
          border: "none",
          color: "var(--color-white)",
          fontSize: "20px",
          fontWeight: "700",
          cursor: "pointer",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          transition: "all 0.3s ease",
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.background =
            "linear-gradient(135deg, var(--color-blue), var(--color-orange))";
          e.currentTarget.style.transform = "scale(1.01)";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background =
            "linear-gradient(135deg, var(--color-navy-end), var(--color-blue))";
          e.currentTarget.style.transform = "scale(1)";
        }}
      >
        <span>{title}</span>
        {isExpanded ? (
          <FaChevronDown size={18} color="var(--color-orange)" />
        ) : (
          <FaChevronRight size={18} color="var(--color-orange)" />
        )}
      </button>
      {isExpanded && (
        <div
          style={{
            background: "rgba(0, 0, 0, 0.2)",
            borderTop: "2px solid var(--color-blue)",
            minHeight: "200px",
          }}
        >
          {children}
        </div>
      )}
    </div>
  );
};

export default function Dashboard() {
  const [aggregateMatchesExpanded, setAggregateMatchesExpanded] =
    useState(false);
  const [jobPostingSummaryExpanded, setJobPostingSummaryExpanded] =
    useState(false);

  return (
    <div style={{ display: "flex", minHeight: "100vh" }}>
      <Sidebar />
      <div style={{ flex: 1, position: "relative", paddingLeft: 0 }}>
        <TopBar />
        <main
          className="frosted"
          style={{
            marginLeft: 64,
            padding: "88px 32px 32px 32px", // top padding to account for TopBar
            minHeight: "100vh",
            position: "relative",
            zIndex: 1,
          }}
        >
          <h1 style={{ color: "var(--color-orange)", marginBottom: 24 }}>
            Dashboard
          </h1>

          <div
            style={{ display: "flex", flexDirection: "column", gap: "16px" }}
          >
            {/* Job Posting Summary Section */}
            <CollapsibleSection
              title="Job Posting Leads"
              isExpanded={jobPostingSummaryExpanded}
              onToggle={() =>
                setJobPostingSummaryExpanded(!jobPostingSummaryExpanded)
              }
            >
              <JobPostingSummary />
            </CollapsibleSection>
          </div>
        </main>
      </div>
    </div>
  );
}
