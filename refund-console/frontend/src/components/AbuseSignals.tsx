import { AlertTriangle, Users, TrendingUp, Package, Truck, Clock } from "lucide-react";

interface AbuseSignalsProps {
  case_data: Record<string, string>;
  customer_360: Record<string, string>;
}

export default function AbuseSignals({ case_data: c, customer_360: cust }: AbuseSignalsProps) {
  const signals: { icon: typeof AlertTriangle; label: string; severity: string }[] = [];

  const refundRate = parseFloat(cust.refund_rate || "0");
  if (refundRate > 0.25) {
    signals.push({
      icon: TrendingUp,
      label: `High refund rate: ${(refundRate * 100).toFixed(1)}%`,
      severity: "high",
    });
  }

  const refunds90d = parseInt(cust.refunds_90d || "0");
  if (refunds90d > 5) {
    signals.push({
      icon: Package,
      label: `${refunds90d} refunds in 90 days`,
      severity: refunds90d > 10 ? "critical" : "high",
    });
  }

  if (cust.coordinated_timing_flag === "true") {
    signals.push({
      icon: Users,
      label: "Household coordination detected",
      severity: "critical",
    });
  }

  if (c.reason_code === "ITEM_NOT_RECEIVED" && c.delivery_confirmed === "true") {
    signals.push({
      icon: Truck,
      label: "Non-delivery claim with photo-confirmed delivery",
      severity: "critical",
    });
  }

  const daysSinceOrder = parseInt(c.days_since_order || "0");
  if (daysSinceOrder > 25 && daysSinceOrder <= 30) {
    signals.push({
      icon: Clock,
      label: `Return window gaming: day ${daysSinceOrder} of 30`,
      severity: "medium",
    });
  }

  const severityColors: Record<string, string> = {
    critical: "bg-critical-light text-critical border-critical/20",
    high: "bg-high-light text-high border-high/20",
    medium: "bg-medium-light text-medium border-medium/20",
  };

  if (signals.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-100 p-5">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-3">
          Abuse Signals
        </h4>
        <p className="text-sm text-gray-400">No abuse signals detected</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5">
      <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
        Abuse Signals ({signals.length})
      </h4>
      <div className="space-y-2">
        {signals.map((signal, i) => (
          <div
            key={i}
            className={`flex items-center gap-3 p-3 rounded-lg border ${severityColors[signal.severity]}`}
          >
            <signal.icon className="w-4 h-4 shrink-0" />
            <p className="text-sm font-medium">{signal.label}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
