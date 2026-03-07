import {
  AlertTriangle,
  CheckCircle,
  XCircle,
  Clock,
  DollarSign,
  TrendingUp,
  Loader2,
  AlertCircle,
} from "lucide-react";
import { useApi } from "../hooks/useApi";
import StatCard from "../components/StatCard";

interface DashboardData {
  metrics: {
    total_cases: string;
    pending_count: string;
    auto_approved_count: string;
    approved_count: string;
    rejected_count: string;
    escalated_count: string;
    avg_risk_score: string;
    potential_leakage: string;
    total_refund_amount: string;
  };
  risk_distribution: Array<{
    abuse_risk_tier: string;
    count: string;
    avg_score: string;
    total_amount: string;
  }>;
}

interface DashboardProps {
  onNavigateToCase: (id: string) => void;
}

const tierColors: Record<string, string> = {
  CRITICAL: "bg-critical",
  HIGH: "bg-high",
  MEDIUM: "bg-medium",
  LOW: "bg-low",
};

export default function Dashboard({ onNavigateToCase: _onNavigateToCase }: DashboardProps) {
  const { data, loading, error } = useApi<DashboardData>("/api/dashboard");

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 text-accent animate-spin" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <AlertCircle className="w-12 h-12 text-danger mx-auto mb-3" />
          <p className="text-sm text-gray-500">Could not load dashboard</p>
          <p className="text-xs text-gray-400 mt-1">{error}</p>
        </div>
      </div>
    );
  }

  if (!data) return null;

  const m = data.metrics;
  const total = parseInt(m.total_cases || "0");
  const pending = parseInt(m.pending_count || "0");
  const approved = parseInt(m.approved_count || "0") + parseInt(m.auto_approved_count || "0");
  const rejected = parseInt(m.rejected_count || "0");
  const escalated = parseInt(m.escalated_count || "0");
  const leakage = parseFloat(m.potential_leakage || "0");
  const avgRisk = parseFloat(m.avg_risk_score || "0");
  const totalAmount = parseFloat(m.total_refund_amount || "0");

  const approvalRate = total > 0 ? ((approved / total) * 100).toFixed(1) : "0";
  const rejectionRate = total > 0 ? ((rejected / total) * 100).toFixed(1) : "0";
  const escalationRate = total > 0 ? ((escalated / total) * 100).toFixed(1) : "0";

  // Calculate max for bar chart
  const maxCount = Math.max(
    ...data.risk_distribution.map((d) => parseInt(d.count || "0")),
    1
  );

  void _onNavigateToCase;

  return (
    <div className="p-6 lg:p-8 max-w-6xl mx-auto">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-navy">Refund Operations Dashboard</h2>
        <p className="text-sm text-gray-400 mt-1">
          Real-time refund abuse monitoring and decisioning metrics
        </p>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Pending Review"
          value={pending.toLocaleString()}
          icon={<Clock className="w-4 h-4 text-warning" />}
          accent="text-warning"
          sub={`of ${total.toLocaleString()} total`}
        />
        <StatCard
          label="Approval Rate"
          value={`${approvalRate}%`}
          icon={<CheckCircle className="w-4 h-4 text-success" />}
          accent="text-success"
        />
        <StatCard
          label="Rejection Rate"
          value={`${rejectionRate}%`}
          icon={<XCircle className="w-4 h-4 text-danger" />}
          accent="text-danger"
        />
        <StatCard
          label="Escalation Rate"
          value={`${escalationRate}%`}
          icon={<AlertTriangle className="w-4 h-4 text-warning" />}
          accent="text-warning"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-8">
        <StatCard
          label="Potential Leakage"
          value={`$${leakage.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          icon={<DollarSign className="w-4 h-4 text-danger" />}
          accent="text-danger"
          sub="Flagged for rejection but not yet actioned"
        />
        <StatCard
          label="Avg Risk Score"
          value={`${(avgRisk * 100).toFixed(1)}%`}
          icon={<TrendingUp className="w-4 h-4 text-accent" />}
          accent="text-accent"
          sub={`Total refund volume: $${totalAmount.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
        />
      </div>

      {/* Risk Distribution */}
      <div className="bg-white rounded-xl border border-gray-100 p-5">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
          Risk Distribution
        </h4>
        <div className="space-y-3">
          {data.risk_distribution.map((tier) => {
            const count = parseInt(tier.count || "0");
            const pct = total > 0 ? ((count / total) * 100).toFixed(1) : "0";
            const barWidth = (count / maxCount) * 100;

            return (
              <div key={tier.abuse_risk_tier} className="space-y-1">
                <div className="flex items-center justify-between text-sm">
                  <span className="font-medium">{tier.abuse_risk_tier}</span>
                  <span className="text-gray-400">
                    {count.toLocaleString()} ({pct}%)
                  </span>
                </div>
                <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className={`h-full rounded-full transition-all ${
                      tierColors[tier.abuse_risk_tier] || "bg-gray-300"
                    }`}
                    style={{ width: `${barWidth}%` }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
