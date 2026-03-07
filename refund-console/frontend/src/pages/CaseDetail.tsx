import { useParams, useNavigate } from "react-router-dom";
import { ArrowLeft, Loader2, AlertCircle } from "lucide-react";
import { useApi } from "../hooks/useApi";
import RiskBadge from "../components/RiskBadge";
import CustomerProfile from "../components/CustomerProfile";
import PolicyCheck from "../components/PolicyCheck";
import AbuseSignals from "../components/AbuseSignals";
import ActionPanel from "../components/ActionPanel";

interface CaseDetailData {
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
  item_condition: string;
  order_amount: string;
  order_date: string;
  payment_method: string;
  product_name: string;
  product_category: string;
  product_price: string;
  is_high_value: string;
  days_since_order: string;
  within_return_window: string;
  delivery_confirmed: string;
  delivery_issues: string;
  refund_to_order_ratio: string;
  risk_score: string;
  recommended_action: string;
  workflow_state: string;
  abuse_risk_tier: string;
  customer_360: Record<string, string>;
  [key: string]: string | Record<string, string>;
}

export default function CaseDetail() {
  const { refundId } = useParams<{ refundId: string }>();
  const navigate = useNavigate();
  const { data, loading, error, refetch } = useApi<CaseDetailData>(
    refundId ? `/api/cases/${refundId}` : null
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-8 h-8 text-accent animate-spin" />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <AlertCircle className="w-12 h-12 text-danger mx-auto mb-3" />
          <p className="text-sm text-gray-500">
            {error || "Case not found"}
          </p>
        </div>
      </div>
    );
  }

  const riskPct = (parseFloat(data.risk_score || "0") * 100).toFixed(0);

  return (
    <div className="p-6 lg:p-8 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-4 mb-6">
        <button
          onClick={() => navigate("/cases")}
          className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
        </button>
        <div className="flex-1">
          <div className="flex items-center gap-3">
            <h2 className="text-2xl font-bold text-navy">{data.refund_id}</h2>
            <RiskBadge tier={data.abuse_risk_tier} size="md" />
          </div>
          <p className="text-sm text-gray-400 mt-1">
            {data.first_name} {data.last_name} &middot; {data.reason_code.replace(/_/g, " ")} &middot; ${parseFloat(data.amount || "0").toFixed(2)}
          </p>
        </div>
        <div className="text-right">
          <p className="text-3xl font-bold text-navy">{riskPct}%</p>
          <p className="text-xs text-gray-400 uppercase tracking-wider">Risk Score</p>
        </div>
      </div>

      {/* Order Details Card */}
      <div className="bg-white rounded-xl border border-gray-100 p-5 mb-4">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-gray-400 mb-4">
          Refund & Order Details
        </h4>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 text-sm">
          <div>
            <p className="text-gray-400 text-xs">Order ID</p>
            <p className="font-mono font-medium">{data.order_id}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Order Amount</p>
            <p className="font-medium">${parseFloat(data.order_amount || "0").toFixed(2)}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Refund Amount</p>
            <p className="font-medium text-danger">${parseFloat(data.amount || "0").toFixed(2)}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Refund/Order Ratio</p>
            <p className="font-medium">{(parseFloat(data.refund_to_order_ratio || "0") * 100).toFixed(0)}%</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Product</p>
            <p className="font-medium">{data.product_name}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Category</p>
            <p className="font-medium">{data.product_category}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Channel</p>
            <p className="font-medium capitalize">{data.channel}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Payment</p>
            <p className="font-medium capitalize">{(data.payment_method || "").replace(/_/g, " ")}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Order Date</p>
            <p className="font-medium">{data.order_date}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Request Date</p>
            <p className="font-medium">{data.request_date}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Item Condition</p>
            <p className="font-medium capitalize">{data.item_condition}</p>
          </div>
          <div>
            <p className="text-gray-400 text-xs">Days Since Order</p>
            <p className="font-medium">{data.days_since_order}</p>
          </div>
        </div>
      </div>

      {/* Two column layout */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="space-y-4">
          <CustomerProfile customer_360={data.customer_360} />
          <PolicyCheck case_data={data as unknown as Record<string, string>} />
        </div>
        <div className="space-y-4">
          <AbuseSignals
            case_data={data as unknown as Record<string, string>}
            customer_360={data.customer_360}
          />
          <ActionPanel
            refundId={data.refund_id}
            recommendedAction={data.recommended_action}
            workflowState={data.workflow_state}
            onActionComplete={refetch}
          />
        </div>
      </div>
    </div>
  );
}
