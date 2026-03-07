interface RiskBadgeProps {
  tier: string;
  size?: "sm" | "md";
}

const tierConfig: Record<string, { bg: string; text: string; label: string }> = {
  CRITICAL: { bg: "bg-critical-light", text: "text-critical", label: "Critical" },
  HIGH: { bg: "bg-high-light", text: "text-high", label: "High" },
  MEDIUM: { bg: "bg-medium-light", text: "text-medium", label: "Medium" },
  LOW: { bg: "bg-low-light", text: "text-low", label: "Low" },
};

export default function RiskBadge({ tier, size = "sm" }: RiskBadgeProps) {
  const config = tierConfig[tier] || tierConfig.LOW;
  const sizeClass = size === "md" ? "px-3 py-1.5 text-sm" : "px-2 py-0.5 text-xs";

  return (
    <span className={`inline-flex items-center gap-1 rounded-full font-semibold ${config.bg} ${config.text} ${sizeClass}`}>
      <span className={`w-1.5 h-1.5 rounded-full ${config.text.replace('text-', 'bg-')}`} />
      {config.label}
    </span>
  );
}
