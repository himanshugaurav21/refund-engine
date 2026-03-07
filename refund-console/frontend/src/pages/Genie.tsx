import { useState, useRef, useEffect } from "react";
import { Send, Loader2, Table2, Code, MessageCircle, Sparkles } from "lucide-react";

interface GenieMessage {
  role: "user" | "genie";
  content: string;
  sql?: string;
  description?: string;
  columns?: string[];
  rows?: string[][];
  status?: string;
  loading?: boolean;
}

const SUGGESTED_QUESTIONS = [
  "Show me the top 20 customers by abuse risk",
  "What is the refund leakage by product category?",
  "Show escalation rates by channel and reason code",
  "What percentage of customers are high or critical risk?",
  "Which reason codes have the highest rejection rate?",
];

export default function Genie() {
  const [messages, setMessages] = useState<GenieMessage[]>([]);
  const [input, setInput] = useState("");
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [showSql, setShowSql] = useState<Record<number, boolean>>({});
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function sendQuestion(question: string) {
    if (!question.trim() || loading) return;

    const userMsg: GenieMessage = { role: "user", content: question };
    const loadingMsg: GenieMessage = { role: "genie", content: "", loading: true };
    setMessages((prev) => [...prev, userMsg, loadingMsg]);
    setInput("");
    setLoading(true);

    try {
      const resp = await fetch("/api/genie/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, conversation_id: conversationId }),
      });
      const data = await resp.json();

      if (!resp.ok) {
        throw new Error(data.detail || "Request failed");
      }

      if (data.conversation_id) {
        setConversationId(data.conversation_id);
      }

      const genieMsg: GenieMessage = {
        role: "genie",
        content: data.text || data.description || "",
        sql: data.sql,
        description: data.description,
        columns: data.columns,
        rows: data.rows,
        status: data.status,
      };

      setMessages((prev) => [...prev.slice(0, -1), genieMsg]);
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : "Unknown error";
      setMessages((prev) => [
        ...prev.slice(0, -1),
        { role: "genie", content: `Error: ${errorMsg}`, status: "FAILED" },
      ]);
    } finally {
      setLoading(false);
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    sendQuestion(input);
  }

  function toggleSql(index: number) {
    setShowSql((prev) => ({ ...prev, [index]: !prev[index] }));
  }

  const isEmpty = messages.length === 0;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-100 bg-white">
        <div className="flex items-center gap-2">
          <Sparkles className="w-5 h-5 text-accent" />
          <h2 className="text-lg font-bold text-navy">Refund Abuse Intelligence</h2>
        </div>
        <p className="text-xs text-gray-400 mt-0.5">
          Ask questions about refund patterns, abuse trends, and risk analytics in natural language
        </p>
      </div>

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto px-6 py-4">
        {isEmpty ? (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <div className="w-14 h-14 rounded-2xl bg-accent/10 flex items-center justify-center mb-4">
              <MessageCircle className="w-7 h-7 text-accent" />
            </div>
            <h3 className="text-base font-semibold text-navy mb-1">
              Ask anything about refund data
            </h3>
            <p className="text-sm text-gray-400 mb-6 max-w-md">
              Powered by Databricks Genie, your questions are converted to SQL and run against the refund analytics tables.
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 max-w-xl w-full">
              {SUGGESTED_QUESTIONS.map((q) => (
                <button
                  key={q}
                  onClick={() => sendQuestion(q)}
                  className="text-left text-sm px-4 py-3 rounded-lg border border-gray-200 hover:border-accent hover:bg-accent/5 transition-colors text-gray-600"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="max-w-4xl mx-auto space-y-4">
            {messages.map((msg, i) => (
              <div
                key={i}
                className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <div
                  className={`max-w-[85%] rounded-xl px-4 py-3 ${
                    msg.role === "user"
                      ? "bg-accent text-white"
                      : "bg-white border border-gray-100"
                  }`}
                >
                  {msg.loading ? (
                    <div className="flex items-center gap-2 text-gray-400 text-sm">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Analyzing your question...
                    </div>
                  ) : (
                    <>
                      {/* Text content */}
                      {msg.content && (
                        <p className={`text-sm whitespace-pre-wrap ${msg.role === "user" ? "" : "text-gray-700"}`}>
                          {msg.content}
                        </p>
                      )}

                      {/* Description */}
                      {msg.description && msg.description !== msg.content && (
                        <p className="text-sm text-gray-500 mt-1">{msg.description}</p>
                      )}

                      {/* SQL toggle */}
                      {msg.sql && (
                        <div className="mt-2">
                          <button
                            onClick={() => toggleSql(i)}
                            className="flex items-center gap-1 text-xs text-accent hover:text-accent-light transition-colors"
                          >
                            <Code className="w-3 h-3" />
                            {showSql[i] ? "Hide SQL" : "Show SQL"}
                          </button>
                          {showSql[i] && (
                            <pre className="mt-1.5 p-3 bg-gray-50 rounded-lg text-xs text-gray-600 overflow-x-auto font-mono">
                              {msg.sql}
                            </pre>
                          )}
                        </div>
                      )}

                      {/* Results table */}
                      {msg.columns && msg.columns.length > 0 && msg.rows && msg.rows.length > 0 && (
                        <div className="mt-3">
                          <div className="flex items-center gap-1 text-xs text-gray-400 mb-1.5">
                            <Table2 className="w-3 h-3" />
                            {msg.rows.length} row{msg.rows.length !== 1 ? "s" : ""}
                          </div>
                          <div className="overflow-x-auto rounded-lg border border-gray-100">
                            <table className="w-full text-xs">
                              <thead>
                                <tr className="bg-gray-50">
                                  {msg.columns.map((col) => (
                                    <th
                                      key={col}
                                      className="px-3 py-2 text-left font-semibold text-gray-500 uppercase tracking-wider whitespace-nowrap"
                                    >
                                      {col}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {msg.rows.slice(0, 50).map((row, ri) => (
                                  <tr
                                    key={ri}
                                    className={ri % 2 === 0 ? "bg-white" : "bg-gray-50/50"}
                                  >
                                    {row.map((cell, ci) => (
                                      <td
                                        key={ci}
                                        className="px-3 py-1.5 text-gray-700 whitespace-nowrap"
                                      >
                                        {cell}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                          {msg.rows.length > 50 && (
                            <p className="text-xs text-gray-400 mt-1">
                              Showing 50 of {msg.rows.length} rows
                            </p>
                          )}
                        </div>
                      )}

                      {/* Error state */}
                      {msg.status === "FAILED" && (
                        <p className="text-xs text-danger mt-1">Query could not be completed.</p>
                      )}
                    </>
                  )}
                </div>
              </div>
            ))}
            <div ref={bottomRef} />
          </div>
        )}
      </div>

      {/* Input bar */}
      <div className="px-6 py-4 border-t border-gray-100 bg-white">
        <form onSubmit={handleSubmit} className="max-w-4xl mx-auto flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about refund patterns, abuse trends, risk analytics..."
            className="flex-1 px-4 py-2.5 rounded-lg border border-gray-200 text-sm focus:outline-none focus:ring-2 focus:ring-accent/30 focus:border-accent"
            disabled={loading}
          />
          <button
            type="submit"
            disabled={!input.trim() || loading}
            className="px-4 py-2.5 bg-accent text-white rounded-lg text-sm font-medium hover:bg-accent/90 disabled:opacity-40 disabled:cursor-not-allowed transition-colors flex items-center gap-2"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Send className="w-4 h-4" />
            )}
          </button>
        </form>
      </div>
    </div>
  );
}
