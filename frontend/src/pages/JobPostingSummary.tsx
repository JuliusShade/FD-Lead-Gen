import React, { useEffect, useMemo, useState } from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  getFilteredRowModel,
  ColumnFiltersState,
  FilterFn,
  Column,
} from "@tanstack/react-table";
import { FaFilter, FaTimesCircle, FaCheck } from "react-icons/fa";
import { createPortal } from "react-dom";
import TableCellWithLinks from "../components/TableCellWithLinks";
import "../index.css";

type JobPostingSummary = {
  id: number;
  company: string;
  position: string | null;
  location: string | null;
  score: number;
  salary_text: string | null;
  job_url: string | null;
  reason: string | null;
  ai_rationale: string | null;
  type_of_role: string[] | null;
  number_of_positions_last_30: string | null;
  hr_contact_name: string | null;
  hr_contact_title: string | null;
  hr_contact_email: string | null;
  hr_contact_linkedin: string | null;
  primary_contact: string | null;
  created_at: string;
  date_processed: string;
};

// Add type for header props
type HeaderProps = {
  column: Column<JobPostingSummary, unknown>;
};

// Add custom filter function for handling arrays of values
const multiSelectFilter: FilterFn<any> = (row, columnId, filterValue) => {
  if (!filterValue || !Array.isArray(filterValue) || filterValue.length === 0)
    return true;
  const value = row.getValue(columnId);
  return filterValue.includes(String(value));
};

// Add custom filter function for arrays (type_of_role)
const arrayFilter: FilterFn<any> = (row, columnId, filterValue) => {
  if (!filterValue || !Array.isArray(filterValue) || filterValue.length === 0)
    return true;
  const value = row.getValue(columnId);
  if (!value || !Array.isArray(value)) return false;

  // Check if any of the filter values match any role in the array
  return filterValue.some((filterVal) =>
    value.some((role) =>
      String(role).toLowerCase().includes(filterVal.toLowerCase())
    )
  );
};

// Add custom date filter function for filtering by day
const dateFilter: FilterFn<any> = (row, columnId, filterValue) => {
  if (!filterValue || !Array.isArray(filterValue) || filterValue.length === 0)
    return true;
  const value = row.getValue(columnId);
  if (!value || typeof value !== "string") return false;

  // Extract the date part directly from the UTC string (YYYY-MM-DD)
  const dateValue = value.split("T")[0];
  return filterValue.includes(dateValue);
};

// Add debounce utility
const debounce = (func: Function, wait: number) => {
  let timeout: NodeJS.Timeout;
  return (...args: any[]) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
};

// Helper function to format date for display
const formatDateForDisplay = (dateString: string) => {
  try {
    const date = new Date(dateString);
    return (
      date.toLocaleDateString("en-US", {
        weekday: "short",
        year: "numeric",
        month: "short",
        day: "numeric",
        timeZone: "UTC", // Display the date in UTC
      }) + " (UTC)"
    );
  } catch (error) {
    return dateString;
  }
};

// Helper function to format array content for display
const formatArrayForDisplay = (arr: string[] | null) => {
  if (!arr || !Array.isArray(arr)) return "";
  return arr.join(" | ");
};

// Update Tooltip component
const Tooltip = ({
  content,
  position,
}: {
  content: string;
  position: { x: number; y: number };
}) => {
  return createPortal(
    <div
      style={{
        position: "fixed",
        top: position.y - 10, // Move up from cursor
        left: position.x + 20, // Move right from cursor
        background: "var(--color-navy-end)",
        border: "1px solid var(--color-orange)",
        borderRadius: "4px",
        padding: "12px",
        maxWidth: "400px",
        maxHeight: "300px",
        overflowY: "auto",
        zIndex: 99999,
        boxShadow: "0 4px 12px rgba(0,0,0,0.2)",
        color: "var(--color-white)",
        fontSize: "14px",
        lineHeight: "1.5",
        opacity: 1,
        transition: "opacity 0.2s ease-in-out",
        pointerEvents: "none",
        transform: "translateY(-100%)", // Move up by its own height
      }}
    >
      {content}
    </div>,
    document.body
  );
};

export default function JobPostingSummary() {
  const [data, setData] = useState<JobPostingSummary[]>([]);
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [activeFilter, setActiveFilter] = useState<string | null>(null);
  const [filterPosition, setFilterPosition] = useState<{
    x: number;
    y: number;
  } | null>(null);
  const [selectedValuesMap, setSelectedValuesMap] = useState<
    Record<string, Set<string>>
  >({}); // Track selected values per column
  const [searchInput, setSearchInput] = useState("");
  const [tooltipContent, setTooltipContent] = useState<string | null>(null);
  const [tooltipPosition, setTooltipPosition] = useState<{
    x: number;
    y: number;
  } | null>(null);
  const [isHovering, setIsHovering] = useState(false);
  const tooltipTimeoutRef = React.useRef<NodeJS.Timeout>();

  useEffect(() => {
    // Fetch from our backend API instead of Supabase
    const apiUrl = import.meta.env.VITE_API_URL || "http://localhost:5001";
    console.log("JobPostingSummary: Fetching data from API...", apiUrl);
    fetch(`${apiUrl}/api/jobs/summary`)
      .then((res) => {
        console.log("JobPostingSummary: Response received", res.status);
        return res.json();
      })
      .then((response) => {
        console.log("JobPostingSummary: Data loaded", response.data?.length, "jobs");
        setData(response.data || []);
      })
      .catch((error) => {
        console.error("JobPostingSummary: Error fetching jobs:", error);
        setData([]);
      });
  }, []);

  const handleFilterClick = (e: React.MouseEvent, columnId: string) => {
    e.stopPropagation();
    const button = e.currentTarget as HTMLElement;
    const rect = button.getBoundingClientRect();

    // Calculate available space
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    // Calculate position to ensure popup stays within viewport
    let x = rect.left;
    let y = rect.bottom;

    // Check if popup would go off right edge
    if (x + 250 > viewportWidth) {
      // 250px is approximate popup width
      x = viewportWidth - 260; // 260px to give some padding
    }

    // Check if popup would go off bottom edge
    if (y + 300 > viewportHeight) {
      // 300px is approximate popup height
      y = rect.top - 310; // Position above the button instead
    }

    setFilterPosition({ x, y });

    if (activeFilter === columnId) {
      setActiveFilter(null);
      setFilterPosition(null);
    } else {
      setActiveFilter(columnId);
      if (!selectedValuesMap[columnId]) {
        setSelectedValuesMap((prev) => ({
          ...prev,
          [columnId]: new Set(),
        }));
      }
    }
    setSearchInput("");
  };

  const getUniqueValues = (columnId: string) => {
    // Get all data instead of just filtered rows to ensure we have all available values
    const values = new Set<string>();

    // Collect values from all data
    data.forEach((row) => {
      const value = row[columnId as keyof JobPostingSummary];
      if (value !== null && value !== undefined) {
        if ((columnId === "created_at" || columnId === "date_processed") && typeof value === "string") {
          // For date columns, extract just the date part (YYYY-MM-DD) directly from UTC string
          try {
            const dateValue = value.split("T")[0];
            values.add(dateValue);
          } catch (error) {
            // If date parsing fails, fall back to original value
            values.add(String(value));
          }
        } else if (columnId === "type_of_role" && Array.isArray(value)) {
          // For type_of_role array, extract individual role types
          value.forEach((role) => {
            if (role && typeof role === "string") {
              values.add(role);
            }
          });
        } else {
          values.add(String(value));
        }
      }
    });

    // Add any currently selected values to ensure they're always available
    const selectedValues = selectedValuesMap[columnId];
    if (selectedValues) {
      selectedValues.forEach((value) => values.add(value));
    }

    return Array.from(values).sort();
  };

  const getFilteredValues = (columnId: string) => {
    const uniqueValues = getUniqueValues(columnId);
    if (!searchInput) return uniqueValues;
    return uniqueValues.filter((value) =>
      value.toLowerCase().includes(searchInput.toLowerCase())
    );
  };

  const handleValueSelect = (columnId: string, value: string) => {
    const newSelectedValues = new Set(selectedValuesMap[columnId] || new Set());

    if (newSelectedValues.has(value)) {
      newSelectedValues.delete(value);
    } else {
      newSelectedValues.add(value);
    }

    setSelectedValuesMap((prev) => ({
      ...prev,
      [columnId]: newSelectedValues,
    }));

    // Update the column filter
    if (newSelectedValues.size > 0) {
      table.getColumn(columnId)?.setFilterValue(Array.from(newSelectedValues));
    } else {
      table.getColumn(columnId)?.setFilterValue(undefined);
    }
  };

  // Reset all filters
  const handleResetFilters = () => {
    // Reset all column filters in the table
    table.resetColumnFilters();

    // Reset our internal state
    setColumnFilters([]);
    setSelectedValuesMap({});
    setActiveFilter(null);
    setFilterPosition(null);
    setSearchInput("");
  };

  // Simplified tooltip handlers
  const handleCellMouseEnter = (e: React.MouseEvent, content: string) => {
    setTooltipContent(content);
    setTooltipPosition({ x: e.clientX, y: e.clientY });
  };

  const handleCellMouseMove = (e: React.MouseEvent) => {
    setTooltipPosition({ x: e.clientX, y: e.clientY });
  };

  const handleCellMouseLeave = () => {
    setTooltipContent(null);
    setTooltipPosition(null);
  };

  const columns = useMemo(
    () => [
      {
        accessorKey: "company",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>COMPANY</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "company")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "position",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>POSITION</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "position")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "ai_rationale",
        header: "AI RATIONALE",
      },
      {
        accessorKey: "type_of_role",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>TYPE OF ROLE</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "type_of_role")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: arrayFilter,
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string[] | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "number_of_positions_last_30",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>POSITIONS LAST 30 DAYS</span>
            <button
              className="filter-button"
              onClick={(e) =>
                handleFilterClick(e, "number_of_positions_last_30")
              }
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "estimated_opportunity",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>ESTIMATED OPPORTUNITY</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "estimated_opportunity")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "primary_contact",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>PRIMARY CONTACT</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "primary_contact")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "score",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>SCORE</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "score")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "location",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>LOCATION</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "location")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "salary_text",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>SALARY</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "salary_text")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "job_url",
        header: "JOB URL",
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "hr_contact_name",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>HR CONTACT</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "hr_contact_name")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "hr_contact_title",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>HR TITLE</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "hr_contact_title")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: multiSelectFilter,
      },
      {
        accessorKey: "hr_contact_linkedin",
        header: "HR LINKEDIN",
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return (
            <TableCellWithLinks content={value} style={{ width: "100%" }} />
          );
        },
      },
      {
        accessorKey: "created_at",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>CREATED AT</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "created_at")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: dateFilter,
      },
      {
        accessorKey: "date_processed",
        header: ({ column }: HeaderProps) => (
          <div
            className="filter-container"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "8px",
              position: "relative",
            }}
          >
            <span>INGESTED AT</span>
            <button
              className="filter-button"
              onClick={(e) => handleFilterClick(e, "date_processed")}
              style={{
                background: "none",
                border: "none",
                padding: 0,
                cursor: "pointer",
              }}
            >
              <FaFilter
                size={14}
                color={column.getFilterValue() ? "var(--color-orange)" : "#888"}
              />
            </button>
          </div>
        ),
        enableColumnFilter: true,
        filterFn: dateFilter,
        cell: ({ getValue }: { getValue: () => any }) => {
          const value = getValue() as string | null;
          return value ? formatDateForDisplay(value) : "";
        },
      },
    ],
    [activeFilter]
  );

  const table = useReactTable({
    data,
    columns,
    state: {
      columnFilters,
    },
    onColumnFiltersChange: setColumnFilters,
    getCoreRowModel: getCoreRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  // Add click outside handler
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      if (target.closest(".filter-input") || target.closest(".filter-button")) {
        return;
      }
      setActiveFilter(null);
      setFilterPosition(null);
      setSearchInput("");
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    <div style={{ padding: 24, position: "relative" }}>
      <div
        style={{
          display: "flex",
          justifyContent: "flex-end",
          alignItems: "center",
          marginBottom: "20px",
        }}
      >
        <button
          style={{
            background: "var(--color-pink)",
            color: "var(--color-white)",
            border: "none",
            borderRadius: "6px",
            padding: "8px 18px",
            fontWeight: 700,
            cursor: "pointer",
            transition: "opacity 0.2s",
            opacity: columnFilters.length > 0 ? 1 : 0.5,
            pointerEvents: columnFilters.length > 0 ? "auto" : "none",
            display: "flex",
            alignItems: "center",
            gap: "8px",
          }}
          onClick={handleResetFilters}
          title={
            columnFilters.length > 0
              ? "Reset all active filters"
              : "No active filters to reset"
          }
        >
          <FaTimesCircle size={14} />
          Reset All Filters
          {columnFilters.length > 0 && (
            <span
              style={{
                background: "rgba(255,255,255,0.2)",
                borderRadius: "50%",
                width: "20px",
                height: "20px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: "12px",
              }}
            >
              {columnFilters.length}
            </span>
          )}
        </button>
      </div>

      <div style={{ position: "relative" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    style={{
                      border: "1px solid var(--color-white)",
                      padding: "12px 8px",
                      background: "var(--color-navy-end)",
                      position: "relative",
                    }}
                  >
                    {flexRender(
                      header.column.columnDef.header,
                      header.getContext()
                    )}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id}>
                {row.getVisibleCells().map((cell) => {
                  // Get the cell value for tooltip
                  const cellValue = cell.getValue();
                  let tooltipText = "";

                  // Format tooltip text based on value type
                  if (Array.isArray(cellValue)) {
                    tooltipText = cellValue.join(", ");
                  } else if (cellValue !== null && cellValue !== undefined) {
                    tooltipText = String(cellValue);
                  }

                  return (
                    <td
                      key={cell.id}
                      style={{
                        border: "1px solid var(--color-white)",
                        padding: 8,
                        maxWidth: "200px",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                        cursor: "pointer",
                        position: "relative",
                      }}
                      onMouseEnter={(e) => handleCellMouseEnter(e, tooltipText)}
                      onMouseMove={handleCellMouseMove}
                      onMouseLeave={handleCellMouseLeave}
                    >
                      {flexRender(
                        cell.column.columnDef.cell,
                        cell.getContext()
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {activeFilter &&
        filterPosition &&
        createPortal(
          <div
            className="filter-input"
            style={{
              position: "fixed",
              top: filterPosition.y + 8,
              left: filterPosition.x,
              padding: "16px",
              background: "var(--color-navy-end)",
              border: "1px solid var(--color-blue)",
              borderRadius: "4px",
              boxShadow: "0 4px 12px rgba(0,0,0,0.2)",
              zIndex: 99999,
              minWidth: "300px",
              maxWidth: "500px",
              maxHeight: "400px",
              display: "flex",
              flexDirection: "column",
              gap: "8px",
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <input
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              placeholder="Search..."
              style={{
                width: "100%",
                padding: "6px 8px",
                border: "1px solid var(--color-blue)",
                borderRadius: "4px",
                background: "var(--color-bg-black)",
                color: "var(--color-white)",
              }}
              onClick={(e) => e.stopPropagation()}
              onFocus={(e) => e.target.select()}
              autoFocus
            />
            <div
              style={{
                maxHeight: "300px",
                overflowY: "auto",
                display: "flex",
                flexDirection: "column",
                gap: "8px",
                padding: "4px",
              }}
            >
              {getFilteredValues(activeFilter).map((value) => (
                <div
                  key={value}
                  onClick={() => handleValueSelect(activeFilter, value)}
                  style={{
                    padding: "10px 12px",
                    cursor: "pointer",
                    display: "flex",
                    alignItems: "flex-start",
                    gap: "8px",
                    borderRadius: "6px",
                    background: selectedValuesMap[activeFilter]?.has(value)
                      ? "var(--color-blue)"
                      : "transparent",
                    color: "var(--color-white)",
                    width: "100%",
                    transition: "all 0.2s ease",
                    border: "1px solid transparent",
                    marginBottom: "2px",
                    boxShadow: selectedValuesMap[activeFilter]?.has(value)
                      ? "0 2px 4px rgba(0, 0, 0, 0.2)"
                      : "none",
                    height: "fit-content",
                  }}
                  onMouseEnter={(e) => {
                    if (!selectedValuesMap[activeFilter]?.has(value)) {
                      e.currentTarget.style.background =
                        "rgba(255, 255, 255, 0.1)";
                      e.currentTarget.style.border =
                        "1px solid var(--color-blue)";
                      e.currentTarget.style.boxShadow =
                        "0 2px 4px rgba(0, 0, 0, 0.2)";
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!selectedValuesMap[activeFilter]?.has(value)) {
                      e.currentTarget.style.background = "transparent";
                      e.currentTarget.style.border = "1px solid transparent";
                      e.currentTarget.style.boxShadow = "none";
                    }
                  }}
                >
                  <FaCheck
                    size={12}
                    color={
                      selectedValuesMap[activeFilter]?.has(value)
                        ? "var(--color-white)"
                        : "transparent"
                    }
                    style={{
                      flexShrink: 0,
                      marginTop: "3px",
                    }}
                  />
                  <span
                    style={{
                      flex: 1,
                      wordBreak: "break-word",
                      whiteSpace: "normal",
                      lineHeight: "1.5",
                      fontSize: "14px",
                      display: "block",
                    }}
                  >
                    {activeFilter === "created_at"
                      ? formatDateForDisplay(value)
                      : value}
                  </span>
                </div>
              ))}
            </div>
          </div>,
          document.body
        )}

      {/* Simplified tooltip portal */}
      {tooltipContent && tooltipPosition && (
        <Tooltip content={tooltipContent} position={tooltipPosition} />
      )}
    </div>
  );
}
