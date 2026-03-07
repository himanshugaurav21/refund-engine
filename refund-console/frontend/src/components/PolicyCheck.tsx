import { CheckCircle, XCircle, AlertTriangle } from "lucide-react";

interface PolicyCheckProps {
  case_data: Record<string, string>;
}

export default function PolicyCheck({ case_data: c }: PolicyCheckProps) {
  const checks = [
    {
      label: "Return Window",
      passed: c.within_return_window === "true",
      detail: `${c.days_since_order || "?"} days since order`,
    },
    {
      label: "Delivery Verified",
      passed: c.delivery_confirmed === "true",
      detail: c.delivery_issues || "No issues",
    },
    {
      label: "Item Condition",
      passed: c.item_condition !== "missing",
      detail: c.item_condition || "Unknown",
    },
    {
      label: "High-Value Item",
      passed: c.is_high_value !== "true",
      detail: c.is_high_value === "true" ? "Requires manual review" : "Standard item",
    },
  ];

  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5">
      <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
        Policy Checks
      </h4>
      <div className="space-y-3">
        {checks.map((check) => (
          <div key={check.label} className="flex items-center gap-3">
            {check.passed ? (
              <CheckCircle className="w-5 h-5 text-success shrink-0" />
            ) : (
              <XCircle className="w-5 h-5 text-danger shrink-0" />
            )}
            <div className="flex-1">
              <p className="text-sm font-medium">{check.label}</p>
              <p className="text-xs text-gray-400">{check.detail}</p>
            </div>
          </div>
        ))}
      </div>

      {c.reason_code === "ITEM_NOT_RECEIVED" && c.delivery_confirmed === "true" && (
        <div className="mt-4 p-3 bg-danger-light rounded-lg flex items-start gap-2">
          <AlertTriangle className="w-4 h-4 text-danger shrink-0 mt-0.5" />
          <p className="text-xs text-danger">
            Customer claims non-delivery but delivery was confirmed with photo proof.
          </p>
        </div>
      )}
    </div>
  );
}
