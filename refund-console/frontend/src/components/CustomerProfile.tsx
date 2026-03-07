import { UserCircle } from "lucide-react";
import RiskBadge from "./RiskBadge";

interface CustomerProfileProps {
  customer_360: Record<string, string>;
}

export default function CustomerProfile({ customer_360: c }: CustomerProfileProps) {
  if (!c || !c.customer_id) return null;

  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5">
      <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
        Customer Profile
      </h4>

      <div className="flex items-center gap-3 mb-4">
        <div className="w-12 h-12 bg-surface-muted rounded-full flex items-center justify-center">
          <UserCircle className="w-7 h-7 text-gray-400" />
        </div>
        <div>
          <p className="font-semibold">{c.first_name} {c.last_name}</p>
          <p className="text-sm text-gray-400">{c.email}</p>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-xs bg-accent/10 text-accent px-2 py-0.5 rounded-full font-medium">
              {c.tier}
            </span>
            <RiskBadge tier={c.risk_tier || "LOW"} />
          </div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 text-sm">
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Lifetime Orders</p>
          <p className="font-semibold">{c.lifetime_orders}</p>
        </div>
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Lifetime Refunds</p>
          <p className="font-semibold">{c.lifetime_refunds}</p>
        </div>
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Refund Rate</p>
          <p className="font-semibold">
            {(parseFloat(c.refund_rate || "0") * 100).toFixed(1)}%
          </p>
        </div>
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Refunds (90d)</p>
          <p className="font-semibold">{c.refunds_90d || "0"}</p>
        </div>
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Lifetime Spend</p>
          <p className="font-semibold">
            ${parseFloat(c.lifetime_spend || "0").toLocaleString(undefined, { maximumFractionDigits: 0 })}
          </p>
        </div>
        <div className="bg-surface-muted rounded-lg p-3">
          <p className="text-xs text-gray-400">Household Coord.</p>
          <p className="font-semibold">
            {c.coordinated_timing_flag === "true" ? "Yes" : "No"}
          </p>
        </div>
      </div>
    </div>
  );
}
