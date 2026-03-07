import { useState } from "react";
import { Loader2, AlertCircle, Filter } from "lucide-react";
import { useApi } from "../hooks/useApi";
import CaseCard from "../components/CaseCard";

interface CaseItem {
  refund_id: string;
  order_id: string;
  customer_id: string;
  first_name: string;
  last_name: string;
  tier: string;
  reason_code: string;
  amount: string;
  channel: string;
  request_date: string;
  risk_score: string;
  recommended_action: string;
  workflow_state: string;
  abuse_risk_tier: string;
  product_name: string;
  product_category: string;
}

interface CasesResponse {
  cases: CaseItem[];
  count: number;
}

export default function CaseList() {
  const [statusFilter, setStatusFilter] = useState("");
  const [riskFilter, setRiskFilter] = useState("");
  const [channelFilter, setChannelFilter] = useState("");

  const params = new URLSearchParams();
  if (statusFilter) params.set("status", statusFilter);
  if (riskFilter) params.set("risk_tier", riskFilter);
  if (channelFilter) params.set("channel", channelFilter);
  params.set("limit", "50");

  const queryString = params.toString();
  const { data, loading, error } = useApi<CasesResponse>(
    `/api/cases?${queryString}`
  );

  return (
    <div className="p-6 lg:p-8 max-w-6xl mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-2xl font-bold text-navy">Refund Cases</h2>
          <p className="text-sm text-gray-400 mt-1">
            {data ? `${data.count} cases` : "Loading..."}
          </p>
        </div>
        <Filter className="w-5 h-5 text-gray-400" />
      </div>

      {/* Filters */}
      <div className="flex flex-wrap gap-3 mb-6">
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent/50"
        >
          <option value="">All Statuses</option>
          <option value="pending_review">Pending Review</option>
          <option value="auto_approved">Auto-approved</option>
          <option value="approved">Approved</option>
          <option value="rejected">Rejected</option>
          <option value="escalated">Escalated</option>
        </select>

        <select
          value={riskFilter}
          onChange={(e) => setRiskFilter(e.target.value)}
          className="bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent/50"
        >
          <option value="">All Risk Levels</option>
          <option value="CRITICAL">Critical</option>
          <option value="HIGH">High</option>
          <option value="MEDIUM">Medium</option>
          <option value="LOW">Low</option>
        </select>

        <select
          value={channelFilter}
          onChange={(e) => setChannelFilter(e.target.value)}
          className="bg-white border border-gray-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent/50"
        >
          <option value="">All Channels</option>
          <option value="online">Online</option>
          <option value="in_store">In-store</option>
          <option value="phone">Phone</option>
          <option value="chat">Chat</option>
        </select>
      </div>

      {/* Cases */}
      {loading && (
        <div className="flex items-center justify-center py-12">
          <Loader2 className="w-8 h-8 text-accent animate-spin" />
        </div>
      )}

      {error && (
        <div className="flex items-center justify-center py-12">
          <div className="text-center">
            <AlertCircle className="w-12 h-12 text-danger mx-auto mb-3" />
            <p className="text-sm text-gray-500">Could not load cases</p>
            <p className="text-xs text-gray-400 mt-1">{error}</p>
          </div>
        </div>
      )}

      {data && (
        <div className="space-y-3">
          {data.cases.map((c) => (
            <CaseCard
              key={c.refund_id}
              refund_id={c.refund_id}
              customer_name={`${c.first_name} ${c.last_name}`}
              reason_code={c.reason_code}
              amount={c.amount}
              risk_score={c.risk_score}
              abuse_risk_tier={c.abuse_risk_tier}
              recommended_action={c.recommended_action}
              workflow_state={c.workflow_state}
              product_name={c.product_name || ""}
              channel={c.channel}
            />
          ))}
          {data.cases.length === 0 && (
            <div className="text-center py-12 text-gray-400">
              No cases match the current filters
            </div>
          )}
        </div>
      )}
    </div>
  );
}
