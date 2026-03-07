import { useState } from "react";
import { CheckCircle, XCircle, ArrowUpCircle, Loader2, Bot } from "lucide-react";

interface ActionPanelProps {
  refundId: string;
  recommendedAction: string;
  workflowState: string;
  onActionComplete: () => void;
}

interface AgentResult {
  recommendation?: {
    action: string;
    confidence: number;
    explanation: string;
    key_factors: string[];
  };
  risk?: {
    risk_score: number;
    risk_tier: string;
    signals: string[];
  };
}

export default function ActionPanel({
  refundId,
  recommendedAction,
  workflowState,
  onActionComplete,
}: ActionPanelProps) {
  const [reason, setReason] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [agentLoading, setAgentLoading] = useState(false);
  const [agentResult, setAgentResult] = useState<AgentResult | null>(null);

  const isActioned = ["approved", "rejected", "escalated"].includes(workflowState);

  const handleAction = async (action: string) => {
    if (!reason.trim()) return;
    setSubmitting(true);
    try {
      const res = await fetch(`/api/cases/${refundId}/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action, reason }),
      });
      if (res.ok) {
        onActionComplete();
      }
    } finally {
      setSubmitting(false);
    }
  };

  const runAgent = async () => {
    setAgentLoading(true);
    try {
      const res = await fetch("/api/agent/decide", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ refund_id: refundId }),
      });
      if (res.ok) {
        const data = await res.json();
        setAgentResult(data);
      }
    } finally {
      setAgentLoading(false);
    }
  };

  const actionColors: Record<string, string> = {
    APPROVE: "text-success",
    REJECT: "text-danger",
    ESCALATE: "text-warning",
  };

  return (
    <div className="bg-white rounded-xl border border-gray-100 p-5">
      <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
        Actions
      </h4>

      {/* AI Recommendation */}
      <div className="mb-4 p-3 bg-accent/5 rounded-lg border border-accent/10">
        <p className="text-xs text-gray-500 mb-1">AI Recommendation</p>
        <p className={`text-lg font-bold ${actionColors[recommendedAction] || ""}`}>
          {recommendedAction}
        </p>
      </div>

      {/* Agent Button */}
      <button
        onClick={runAgent}
        disabled={agentLoading}
        className="w-full mb-4 flex items-center justify-center gap-2 px-4 py-2.5 bg-accent/10 hover:bg-accent/20 text-accent rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
      >
        {agentLoading ? (
          <Loader2 className="w-4 h-4 animate-spin" />
        ) : (
          <Bot className="w-4 h-4" />
        )}
        {agentLoading ? "Analyzing..." : "Run AI Analysis"}
      </button>

      {/* Agent Result */}
      {agentResult?.recommendation && (
        <div className="mb-4 p-3 bg-surface-muted rounded-lg text-sm space-y-2">
          <div className="flex items-center justify-between">
            <span className={`font-bold ${actionColors[agentResult.recommendation.action] || ""}`}>
              {agentResult.recommendation.action}
            </span>
            <span className="text-xs text-gray-400">
              {(agentResult.recommendation.confidence * 100).toFixed(0)}% confidence
            </span>
          </div>
          <p className="text-gray-600">{agentResult.recommendation.explanation}</p>
          {agentResult.recommendation.key_factors?.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {agentResult.recommendation.key_factors.map((f, i) => (
                <span key={i} className="text-xs bg-white px-2 py-0.5 rounded border border-gray-200">
                  {f}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {isActioned ? (
        <div className="p-3 bg-surface-muted rounded-lg text-center">
          <p className="text-sm text-gray-500">
            Case has been <span className="font-semibold">{workflowState}</span>
          </p>
        </div>
      ) : (
        <>
          <textarea
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="Enter reason for action..."
            className="w-full border border-gray-200 rounded-lg p-3 text-sm mb-3 focus:outline-none focus:border-accent/50 resize-none"
            rows={3}
          />

          <div className="grid grid-cols-3 gap-2">
            <button
              onClick={() => handleAction("approved")}
              disabled={submitting || !reason.trim()}
              className="flex items-center justify-center gap-1.5 px-3 py-2.5 bg-success/10 hover:bg-success/20 text-success rounded-lg text-sm font-medium transition-colors disabled:opacity-30"
            >
              <CheckCircle className="w-4 h-4" />
              Approve
            </button>
            <button
              onClick={() => handleAction("rejected")}
              disabled={submitting || !reason.trim()}
              className="flex items-center justify-center gap-1.5 px-3 py-2.5 bg-danger/10 hover:bg-danger/20 text-danger rounded-lg text-sm font-medium transition-colors disabled:opacity-30"
            >
              <XCircle className="w-4 h-4" />
              Reject
            </button>
            <button
              onClick={() => handleAction("escalated")}
              disabled={submitting || !reason.trim()}
              className="flex items-center justify-center gap-1.5 px-3 py-2.5 bg-warning/10 hover:bg-warning/20 text-warning rounded-lg text-sm font-medium transition-colors disabled:opacity-30"
            >
              <ArrowUpCircle className="w-4 h-4" />
              Escalate
            </button>
          </div>
        </>
      )}
    </div>
  );
}
