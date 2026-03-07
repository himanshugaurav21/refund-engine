import { BrowserRouter, Routes, Route, NavLink, useNavigate } from "react-router-dom";
import {
  LayoutDashboard,
  FileText,
  Shield,
  MessageSquareWarning,
  Sparkles,
} from "lucide-react";
import Dashboard from "./pages/Dashboard";
import CaseList from "./pages/CaseList";
import CaseDetail from "./pages/CaseDetail";
import Feedback from "./pages/Feedback";
import Genie from "./pages/Genie";

function Sidebar() {
  const navItems = [
    { to: "/", icon: LayoutDashboard, label: "Dashboard" },
    { to: "/cases", icon: FileText, label: "Cases" },
    { to: "/genie", icon: Sparkles, label: "Genie" },
    { to: "/feedback", icon: MessageSquareWarning, label: "Feedback" },
  ];

  return (
    <aside className="w-64 bg-navy text-white flex flex-col shrink-0">
      <div className="px-6 py-5 border-b border-white/10 flex items-center gap-3">
        <div className="w-9 h-9 bg-accent rounded-lg flex items-center justify-center">
          <Shield className="w-5 h-5 text-white" />
        </div>
        <div>
          <h1 className="text-base font-bold tracking-tight">Refund</h1>
          <p className="text-[11px] text-white/50 -mt-0.5">Console</p>
        </div>
      </div>

      <nav className="flex-1 px-3 py-4 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                isActive
                  ? "bg-accent/15 text-accent-light"
                  : "text-white/60 hover:text-white hover:bg-white/5"
              }`
            }
          >
            <item.icon className="w-[18px] h-[18px]" />
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="px-4 py-4 border-t border-white/10">
        <p className="text-[10px] text-white/30 uppercase tracking-wider">
          Refund Abuse Decisioning
        </p>
      </div>
    </aside>
  );
}

function AppRoutes() {
  const navigate = useNavigate();

  return (
    <Routes>
      <Route path="/" element={<Dashboard onNavigateToCase={(id) => navigate(`/cases/${id}`)} />} />
      <Route path="/cases" element={<CaseList />} />
      <Route path="/cases/:refundId" element={<CaseDetail />} />
      <Route path="/genie" element={<Genie />} />
      <Route path="/feedback" element={<Feedback />} />
    </Routes>
  );
}

function App() {
  return (
    <BrowserRouter>
      <div className="flex h-screen bg-surface-muted">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <AppRoutes />
        </main>
      </div>
    </BrowserRouter>
  );
}

export default App;
