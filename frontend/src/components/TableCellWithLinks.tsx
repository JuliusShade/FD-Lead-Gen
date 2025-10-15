import React, { useState, useCallback } from "react";
import { createPortal } from "react-dom";
import { FaExternalLinkAlt, FaLink } from "react-icons/fa";

interface TableCellWithLinksProps {
  content: string | string[] | null;
  className?: string;
  style?: React.CSSProperties;
}

interface DetectedURL {
  url: string;
  displayText: string;
  fullText: string;
}

/**
 * Component that detects URLs in table cell content and makes them clickable
 * Supports both string content and arrays of strings
 * Shows a popover on hover with clickable links
 */
const TableCellWithLinks: React.FC<TableCellWithLinksProps> = ({
  content,
  className = "",
  style = {},
}) => {
  const [showPopover, setShowPopover] = useState(false);
  const [popoverPosition, setPopoverPosition] = useState<{
    x: number;
    y: number;
  } | null>(null);
  const [isHoveringCell, setIsHoveringCell] = useState(false);
  const [isHoveringPopover, setIsHoveringPopover] = useState(false);
  const [lastMousePosition, setLastMousePosition] = useState<{
    x: number;
    y: number;
  } | null>(null);
  const hideTimeoutRef = React.useRef<NodeJS.Timeout>();

  // Enhanced URL detection regex that captures URLs and surrounding context
  const urlRegex = /(https?:\/\/[^\s\|\)]+)/gi;

  /**
   * Detects URLs in text content and extracts context
   */
  const detectURLs = useCallback((text: string): DetectedURL[] => {
    const urls: DetectedURL[] = [];
    const matches = text.matchAll(urlRegex);

    for (const match of matches) {
      const url = match[1];
      const matchIndex = match.index || 0;

      // Extract context around the URL (up to 50 chars before and after)
      const contextStart = Math.max(0, matchIndex - 50);
      const contextEnd = Math.min(text.length, matchIndex + url.length + 50);
      const context = text.substring(contextStart, contextEnd);

      // Try to extract a meaningful display name from the context
      let displayText = url;

      // Look for patterns like "Channel Name (views) - Title | URL"
      const beforeUrl = text.substring(contextStart, matchIndex).trim();
      if (beforeUrl) {
        // Extract the last meaningful part before the URL
        const parts = beforeUrl
          .split(/[\|\-\(\)]/)
          .filter((part) => part.trim());
        if (parts.length > 0) {
          displayText = parts[parts.length - 1].trim();
          // If it's just numbers (like view counts), try the previous part
          if (/^\d+(?:,\d+)*$/.test(displayText) && parts.length > 1) {
            displayText = parts[parts.length - 2].trim();
          }
        }
      }

      // Fallback: try to get hostname for cleaner display
      if (displayText === url) {
        try {
          const urlObj = new URL(url);
          displayText = urlObj.hostname.replace("www.", "");
        } catch {
          // Keep original URL if parsing fails
        }
      }

      urls.push({
        url,
        displayText:
          displayText.length > 60
            ? displayText.substring(0, 60) + "..."
            : displayText,
        fullText: context,
      });
    }

    return urls;
  }, []);

  /**
   * Process content and detect all URLs
   */
  const processContent = useCallback(() => {
    if (!content) return { hasURLs: false, urls: [], displayText: "" };

    let textToProcess = "";

    if (Array.isArray(content)) {
      textToProcess = content.join(" | ");
    } else {
      textToProcess = String(content);
    }

    const urls = detectURLs(textToProcess);

    return {
      hasURLs: urls.length > 0,
      urls,
      displayText: textToProcess,
    };
  }, [content, detectURLs]);

  const { hasURLs, urls, displayText } = processContent();

  // Function to check if mouse is moving toward the popover
  const isMovingTowardPopover = React.useCallback(
    (currentPos: { x: number; y: number }) => {
      if (!popoverPosition || !lastMousePosition) return false;

      const popoverTop = Math.max(10, popoverPosition.y - 60);
      const popoverLeft = Math.min(
        Math.max(10, popoverPosition.x + 10),
        window.innerWidth - 420
      );

      // Calculate if mouse is moving closer to popover area
      const oldDistance = Math.sqrt(
        Math.pow(lastMousePosition.x - popoverLeft, 2) +
          Math.pow(lastMousePosition.y - popoverTop, 2)
      );
      const newDistance = Math.sqrt(
        Math.pow(currentPos.x - popoverLeft, 2) +
          Math.pow(currentPos.y - popoverTop, 2)
      );

      return newDistance < oldDistance;
    },
    [popoverPosition, lastMousePosition]
  );

  // Effect to control popover visibility based on hover states
  React.useEffect(() => {
    // Clear any existing timeout
    if (hideTimeoutRef.current) {
      clearTimeout(hideTimeoutRef.current);
      hideTimeoutRef.current = undefined;
    }

    if (isHoveringCell || isHoveringPopover) {
      // Show popover if hovering either cell or popover
      if (!showPopover && hasURLs) {
        setShowPopover(true);
      }
    } else {
      // Neither cell nor popover is hovered
      if (showPopover) {
        // Give a very short delay only if last movement was toward popover
        const wasMovingTowardPopover =
          lastMousePosition &&
          popoverPosition &&
          isMovingTowardPopover(lastMousePosition);

        if (wasMovingTowardPopover) {
          // Brief delay if moving toward popover
          hideTimeoutRef.current = setTimeout(() => {
            setShowPopover(false);
            setPopoverPosition(null);
          }, 75);
        } else {
          // Hide immediately if moving away or quickly across cells
          setShowPopover(false);
          setPopoverPosition(null);
        }
      }
    }
  }, [
    isHoveringCell,
    isHoveringPopover,
    hasURLs,
    showPopover,
    isMovingTowardPopover,
    lastMousePosition,
    popoverPosition,
  ]);

  /**
   * Handle cell mouse enter
   */
  const handleCellMouseEnter = useCallback(
    (e: React.MouseEvent) => {
      if (hasURLs) {
        setIsHoveringCell(true);
        setPopoverPosition({ x: e.clientX, y: e.clientY });
      }
    },
    [hasURLs]
  );

  /**
   * Handle cell mouse move - update popover position and track movement
   */
  const handleCellMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (hasURLs && isHoveringCell) {
        const newPosition = { x: e.clientX, y: e.clientY };
        setPopoverPosition(newPosition);
        setLastMousePosition(newPosition);
      }
    },
    [hasURLs, isHoveringCell]
  );

  /**
   * Handle cell mouse leave
   */
  const handleCellMouseLeave = useCallback(() => {
    setIsHoveringCell(false);
  }, []);

  /**
   * Handle popover mouse enter
   */
  const handlePopoverMouseEnter = useCallback(() => {
    setIsHoveringPopover(true);
  }, []);

  /**
   * Handle popover mouse leave
   */
  const handlePopoverMouseLeave = useCallback(() => {
    setIsHoveringPopover(false);
  }, []);

  // Cleanup timeout on unmount
  React.useEffect(() => {
    return () => {
      if (hideTimeoutRef.current) {
        clearTimeout(hideTimeoutRef.current);
      }
    };
  }, []);

  /**
   * Handle URL click - open in new tab
   */
  const handleURLClick = useCallback(
    (url: string, e: React.MouseEvent | React.KeyboardEvent) => {
      e.preventDefault();
      e.stopPropagation();
      window.open(url, "_blank", "noopener,noreferrer");
    },
    []
  );

  /**
   * Render the popover with clickable URLs
   */
  const renderPopover = () => {
    if (!showPopover || !popoverPosition || !hasURLs) return null;

    const popoverTop = Math.max(10, popoverPosition.y - 60);
    const popoverLeft = Math.min(
      Math.max(10, popoverPosition.x + 10),
      window.innerWidth - 420
    );

    return createPortal(
      <>
        {/* Invisible bridge area to make mouse movement easier */}
        <div
          style={{
            position: "fixed",
            top: Math.min(popoverPosition.y - 20, popoverTop),
            left: Math.min(popoverPosition.x - 10, popoverLeft),
            width: Math.max(
              Math.abs(popoverLeft - popoverPosition.x) + 50,
              100
            ),
            height: Math.max(Math.abs(popoverTop - popoverPosition.y) + 40, 80),
            zIndex: 99998,
            pointerEvents: "auto",
            background: "transparent",
            // Uncomment next line for debugging - shows the bridge area
            // background: "rgba(255, 0, 0, 0.1)",
          }}
          onMouseEnter={handlePopoverMouseEnter}
          onMouseLeave={handlePopoverMouseLeave}
        />

        {/* Main popover content */}
        <div
          style={{
            position: "fixed",
            top: popoverTop,
            left: popoverLeft,
            background: "var(--color-navy-end)",
            border: "1px solid var(--color-orange)",
            borderRadius: "6px",
            padding: "12px",
            maxWidth: Math.min(400, window.innerWidth - 40), // Responsive max width
            maxHeight: Math.min(300, window.innerHeight - 60), // Responsive max height
            overflowY: "auto",
            zIndex: 99999,
            boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
            color: "var(--color-white)",
            fontSize: "14px",
            lineHeight: "1.5",
            pointerEvents: "auto",
          }}
          onMouseEnter={handlePopoverMouseEnter}
          onMouseLeave={handlePopoverMouseLeave}
        >
          <div
            style={{
              marginBottom: "8px",
              fontWeight: "600",
              color: "var(--color-orange)",
            }}
          >
            {urls.length > 1 ? `${urls.length} Links Found` : "Link Found"}
          </div>

          {urls.map((urlData, index) => (
            <div
              key={index}
              style={{
                marginBottom: index < urls.length - 1 ? "12px" : "0",
                padding: "8px",
                border: "1px solid rgba(255,255,255,0.1)",
                borderRadius: "4px",
                background: "rgba(255,255,255,0.05)",
              }}
            >
              <div
                style={{ marginBottom: "4px", fontSize: "12px", color: "#ccc" }}
              >
                {urlData.fullText.length > 100
                  ? urlData.fullText.substring(0, 100) + "..."
                  : urlData.fullText}
              </div>

              <button
                onClick={(e) => handleURLClick(urlData.url, e)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleURLClick(urlData.url, e);
                  }
                }}
                aria-label={`Open link: ${urlData.displayText}`}
                title={`Click to open: ${urlData.url}`}
                style={{
                  background: "var(--color-blue)",
                  color: "var(--color-white)",
                  border: "none",
                  borderRadius: "4px",
                  padding: "6px 12px",
                  cursor: "pointer",
                  fontSize: "12px",
                  display: "flex",
                  alignItems: "center",
                  gap: "6px",
                  transition: "all 0.2s ease",
                  width: "100%",
                  textAlign: "left",
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = "var(--color-orange)";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = "var(--color-blue)";
                }}
                onFocus={(e) => {
                  e.currentTarget.style.background = "var(--color-orange)";
                  e.currentTarget.style.outline =
                    "2px solid var(--color-white)";
                  e.currentTarget.style.outlineOffset = "2px";
                }}
                onBlur={(e) => {
                  e.currentTarget.style.background = "var(--color-blue)";
                  e.currentTarget.style.outline = "none";
                }}
              >
                <FaExternalLinkAlt size={10} />
                <span
                  style={{
                    flex: 1,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                >
                  {urlData.displayText}
                </span>
              </button>
            </div>
          ))}
        </div>
      </>,
      document.body
    );
  };

  return (
    <>
      <div
        className={className}
        style={{
          ...style,
          cursor: hasURLs ? "pointer" : "default",
          position: "relative",
          display: "flex",
          alignItems: "center",
          gap: "8px",
        }}
        onMouseEnter={handleCellMouseEnter}
        onMouseMove={handleCellMouseMove}
        onMouseLeave={handleCellMouseLeave}
      >
        <span
          style={{
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            flex: 1,
          }}
        >
          {displayText || ""}
        </span>

        {hasURLs && (
          <FaLink
            size={12}
            color="var(--color-orange)"
            style={{
              flexShrink: 0,
              opacity: 0.7,
            }}
          />
        )}
      </div>

      {renderPopover()}
    </>
  );
};

export default TableCellWithLinks;
