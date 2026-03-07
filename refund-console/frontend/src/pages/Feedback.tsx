import { useState } from "react";
import { AlertTriangle, CheckCircle, Send, Loader2 } from "lucide-react";

export default function Feedback() {
  const [refundId, setRefundId] = useState("");
  const [feedbackType, setFeedbackType] = useState<"false_positive" | "missed_abuse">("false_positive");
  const [notes, setNotes] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!refundId.trim()) return;

    setSubmitting(true);
    try {
      const res = await fetch("/api/feedback", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          refund_id: refundId,
          feedback_type: feedbackType,
          notes,
        }),
      });
      if (res.ok) {
        setSubmitted(true);
        setTimeout(() => {
          setSubmitted(false);
          setRefundId("");
          setNotes("");
        }, 3000);
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="p-6 lg:p-8 max-w-3xl mx-auto">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-navy">Override Feedback</h2>
        <p className="text-sm text-gray-400 mt-1">
          Report false positives or missed abuse to improve the decisioning model
        </p>
      </div>

      {/* Info cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
        <div
          onClick={() => setFeedbackType("false_positive")}
          className={`p-5 rounded-xl border cursor-pointer transition-all ${
            feedbackType === "false_positive"
              ? "border-warning bg-warning-light"
              : "border-gray-100 bg-white hover:border-warning/30"
          }`}
        >
          <AlertTriangle className={`w-8 h-8 mb-3 ${
            feedbackType === "false_positive" ? "text-warning" : "text-gray-300"
          }`} />
          <h3 className="font-semibold text-sm mb-1">False Positive</h3>
          <p className="text-xs text-gray-500">
            AI flagged as abuse, but the refund was legitimate. The customer was wrongly denied or escalated.
          </p>
        </div>

        <div
          onClick={() => setFeedbackType("missed_abuse")}
          className={`p-5 rounded-xl border cursor-pointer transition-all ${
            feedbackType === "missed_abuse"
              ? "border-danger bg-danger-light"
              : "border-gray-100 bg-white hover:border-danger/30"
          }`}
        >
          <AlertTriangle className={`w-8 h-8 mb-3 ${
            feedbackType === "missed_abuse" ? "text-danger" : "text-gray-300"
          }`} />
          <h3 className="font-semibold text-sm mb-1">Missed Abuse</h3>
          <p className="text-xs text-gray-500">
            AI approved or did not flag, but the refund was actually abusive. The system missed a pattern.
          </p>
        </div>
      </div>

      {/* Form */}
      <form onSubmit={handleSubmit} className="bg-white rounded-xl border border-gray-100 p-6">
        <div className="space-y-4">
          <div>
            <label className="text-xs font-semibold uppercase tracking-wider text-gray-400 block mb-2">
              Refund ID
            </label>
            <input
              type="text"
              value={refundId}
              onChange={(e) => setRefundId(e.target.value)}
              placeholder="REF-0000001"
              className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-accent/50"
            />
          </div>

          <div>
            <label className="text-xs font-semibold uppercase tracking-wider text-gray-400 block mb-2">
              Notes
            </label>
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              placeholder="Describe why this was a false positive or missed abuse..."
              className="w-full border border-gray-200 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-accent/50 resize-none"
              rows={4}
            />
          </div>

          <button
            type="submit"
            disabled={submitting || !refundId.trim()}
            className="w-full flex items-center justify-center gap-2 px-4 py-3 bg-accent hover:bg-accent-muted text-white rounded-lg text-sm font-medium transition-colors disabled:opacity-50"
          >
            {submitting ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : submitted ? (
              <CheckCircle className="w-4 h-4" />
            ) : (
              <Send className="w-4 h-4" />
            )}
            {submitting ? "Submitting..." : submitted ? "Submitted!" : "Submit Feedback"}
          </button>
        </div>
      </form>
    </div>
  );
}
