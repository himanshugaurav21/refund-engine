import { useNavigate } from "react-router-dom";
import { ChevronRight } from "lucide-react";
import RiskBadge from "./RiskBadge";

interface CaseCardProps {
  refund_id: string;
  customer_name: string;
  reason_code: string;
  amount: string;
  risk_score: string;
  abuse_risk_tier: string;
  recommended_action: string;
  workflow_state: string;
  product_name: string;
  channel: string;
}

const actionColors: Record<string, string> = {
  APPROVE: "text-success",
  REJECT: "text-danger",
  ESCALATE: "text-warning",
};

const stateLabels: Record<string, string> = {
  auto_approved: "Auto-approved",
  pending_review: "Pending Review",
  approved: "Approved",
  rejected: "Rejected",
  escalated: "Escalated",
};

export default function CaseCard(props: CaseCardProps) {
  const navigate = useNavigate();
  const riskPct = (parseFloat(props.risk_score || "0") * 100).toFixed(0);

  return (
    <div
      onClick={() => navigate(`/cases/${props.refund_id}`)}
      className="bg-white rounded-xl border border-gray-100 p-4 hover:shadow-md hover:border-accent/20 transition-all cursor-pointer"
    >
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-3">
          <span className="text-sm font-mono font-semibold text-navy">
            {props.refund_id}
          </span>
          <RiskBadge tier={props.abuse_risk_tier} />
        </div>
        <ChevronRight className="w-4 h-4 text-gray-300" />
      </div>

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 text-sm">
        <div>
          <p className="text-gray-400 text-xs">Customer</p>
          <p className="font-medium">{props.customer_name}</p>
        </div>
        <div>
          <p className="text-gray-400 text-xs">Amount</p>
          <p className="font-medium">${parseFloat(props.amount || "0").toFixed(2)}</p>
        </div>
        <div>
          <p className="text-gray-400 text-xs">Reason</p>
          <p className="font-medium">{props.reason_code.replace(/_/g, " ")}</p>
        </div>
        <div>
          <p className="text-gray-400 text-xs">Risk Score</p>
          <p className="font-medium">{riskPct}%</p>
        </div>
      </div>

      <div className="flex items-center justify-between mt-3 pt-3 border-t border-gray-50">
        <div className="flex items-center gap-4 text-xs text-gray-400">
          <span>{props.channel}</span>
          <span>{props.product_name}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className={`text-xs font-semibold ${actionColors[props.recommended_action] || ""}`}>
            {props.recommended_action}
          </span>
          <span className="text-xs text-gray-400">
            {stateLabels[props.workflow_state] || props.workflow_state}
          </span>
        </div>
      </div>
    </div>
  );
}
