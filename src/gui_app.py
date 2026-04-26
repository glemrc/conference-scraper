import os
import sys
import queue
import threading
import logging
import time
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import customtkinter as ctk

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import pandas as pd

# Ensure we can import the src modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from main import main as run_pipeline, write_excel_report
from config import DATE_KEYS

# -------------------------------------------------------------
# LOGGING HANDLER
# -------------------------------------------------------------
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put(msg)
        except Exception:
            self.handleError(record)


# -------------------------------------------------------------
# MAIN APPLICATION
# -------------------------------------------------------------
class ConferenceScraperApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Conference Intelligence - Scraper v2")
        self.geometry("1400x900")
        
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # State
        self.log_queue = queue.Queue()
        self.update_queue = queue.Queue()
        self.worker_thread = None
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        
        self.source_urls = None  # Explicit list of URLs to process
        self.is_running = False
        
        self.final_records = []
        self.final_change_report = None
        self.url_item_map: dict = {}
        self._start_time: float = 0.0

        self.setup_logging()
        self.build_sidebar()
        self.build_main_area()
        self.after(100, self.poll_queues)

    def setup_logging(self):
        root_logger = logging.getLogger()
        handler = QueueHandler(self.log_queue)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
        root_logger.addHandler(handler)
        root_logger.setLevel(logging.INFO)

    def build_sidebar(self):
        self.sidebar_frame = ctk.CTkFrame(self, width=280, corner_radius=0)
        self.sidebar_frame.grid(row=0, column=0, rowspan=2, sticky="nsew")
        self.sidebar_frame.grid_rowconfigure(8, weight=1) 

        # Title
        self.logo_label = ctk.CTkLabel(self.sidebar_frame, text="Conference\nIntelligence", font=ctk.CTkFont(size=24, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 30))

        # File Selection Options
        self.file_label = ctk.CTkLabel(self.sidebar_frame, text="Source Links:", font=ctk.CTkFont(weight="bold"))
        self.file_label.grid(row=1, column=0, padx=20, pady=(0, 5), sticky="w")
        
        self.btn_load_file = ctk.CTkButton(self.sidebar_frame, text="Load .xlsx / .csv / .txt", command=self.load_file, fg_color="#303A52", hover_color="#202A42")
        self.btn_load_file.grid(row=2, column=0, padx=20, pady=(0, 10), sticky="ew")

        self.btn_paste = ctk.CTkButton(self.sidebar_frame, text="Paste Links", command=self.open_paste_modal, fg_color="#303A52", hover_color="#202A42")
        self.btn_paste.grid(row=3, column=0, padx=20, pady=(0, 10), sticky="ew")
        
        self.file_display = ctk.CTkLabel(self.sidebar_frame, text="Using Database default", text_color="#aaaaaa", font=ctk.CTkFont(size=12))
        self.file_display.grid(row=4, column=0, padx=20, pady=(0, 15), sticky="w")

        # Extraction Mode Option
        self.lbl_mode = ctk.CTkLabel(self.sidebar_frame, text="Extraction Mode:", font=ctk.CTkFont(weight="bold"))
        self.lbl_mode.grid(row=5, column=0, padx=20, pady=(0, 5), sticky="w")
        
        self.mode_var = ctk.StringVar(value="All (Dates + Topics)")
        self.opt_mode = ctk.CTkOptionMenu(self.sidebar_frame, values=["All (Dates + Topics)", "Dates Only", "Topics Only"], variable=self.mode_var)
        self.opt_mode.grid(row=6, column=0, padx=20, pady=(0, 15), sticky="ew")

        # Controls
        self.controls_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        self.controls_frame.grid(row=7, column=0, padx=20, pady=10, sticky="ew")

        self.btn_run = ctk.CTkButton(self.controls_frame, text="▶ Run", fg_color="#28a745", hover_color="#218838", command=self.start_pipeline)
        self.btn_run.pack(fill="x", pady=5)
        
        self.btn_pause = ctk.CTkButton(self.controls_frame, text="⏸ Pause", fg_color="#ffc107", hover_color="#e0a800", text_color="black", state="disabled", command=self.pause_pipeline)
        self.btn_pause.pack(fill="x", pady=5)
        
        self.btn_stop = ctk.CTkButton(self.controls_frame, text="⏹ Stop", fg_color="#dc3545", hover_color="#c82333", state="disabled", command=self.stop_pipeline)
        self.btn_stop.pack(fill="x", pady=5)

        # Bottom actions
        self.btn_export = ctk.CTkButton(self.sidebar_frame, text="💾 Export Excel", fg_color="#0056b3", hover_color="#004085", state="disabled", command=self.export_excel)
        self.btn_export.grid(row=9, column=0, padx=20, pady=(0, 10), sticky="ew")

        # Stats
        self.stats_label = ctk.CTkLabel(self.sidebar_frame, text="Quick Stats", font=ctk.CTkFont(weight="bold"))
        self.stats_label.grid(row=10, column=0, padx=20, pady=(10, 0), sticky="w")
        self.lbl_processed = ctk.CTkLabel(self.sidebar_frame, text="Processed: 0 / 0")
        self.lbl_processed.grid(row=11, column=0, padx=20, sticky="w")
        self.lbl_errors = ctk.CTkLabel(self.sidebar_frame, text="Errors: 0", text_color="#ef4444")
        self.lbl_errors.grid(row=12, column=0, padx=20, pady=(0, 20), sticky="w")

    def build_main_area(self):
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")
        self.main_frame.grid_rowconfigure(0, weight=6)
        self.main_frame.grid_rowconfigure(1, weight=4)
        self.main_frame.grid_columnconfigure(0, weight=1)

        self.tabview = ctk.CTkTabview(self.main_frame)
        self.tabview.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        self.tabview.add("URL Queue")
        self.tabview.add("Data Preview")
        self.tabview.add("Extraction Stats")
        self.tabview.add("Manual Review")

        for name in ["URL Queue", "Data Preview", "Extraction Stats", "Manual Review"]:
            self.tabview.tab(name).grid_rowconfigure(0, weight=1)
            self.tabview.tab(name).grid_columnconfigure(0, weight=1)

        self.setup_url_queue(self.tabview.tab("URL Queue"))
        self.setup_treeview(self.tabview.tab("Data Preview"))
        self.setup_chart(self.tabview.tab("Extraction Stats"))
        self.setup_manual_review(self.tabview.tab("Manual Review"))

        # Logs
        self.log_frame = ctk.CTkFrame(self.main_frame)
        self.log_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        self.log_frame.grid_rowconfigure(1, weight=1)
        self.log_frame.grid_columnconfigure(0, weight=1)

        # Log header with controls
        log_header = ctk.CTkFrame(self.log_frame, fg_color="transparent")
        log_header.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))
        log_header.grid_columnconfigure(0, weight=1)

        log_lbl = ctk.CTkLabel(log_header, text="System Logs", font=ctk.CTkFont(weight="bold"))
        log_lbl.grid(row=0, column=0, sticky="w")

        btn_copy_log = ctk.CTkButton(log_header, text="📋 Copy", width=72, height=24,
                                      fg_color="#303A52", hover_color="#202A42",
                                      font=ctk.CTkFont(size=11), command=self.copy_log)
        btn_copy_log.grid(row=0, column=1, padx=(5, 0))

        btn_clear_log = ctk.CTkButton(log_header, text="🗑 Clear", width=72, height=24,
                                       fg_color="#303A52", hover_color="#202A42",
                                       font=ctk.CTkFont(size=11), command=self.clear_log)
        btn_clear_log.grid(row=0, column=2, padx=(5, 0))

        self.log_textbox = ctk.CTkTextbox(self.log_frame, state="disabled", fg_color="#1e1e1e", font=("Consolas", 12))
        self.log_textbox.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)

        # Progress row with ETA
        prog_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        prog_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))
        prog_frame.grid_columnconfigure(0, weight=1)

        self.progress_bar = ctk.CTkProgressBar(prog_frame)
        self.progress_bar.grid(row=0, column=0, sticky="ew")
        self.progress_bar.set(0)

        self.lbl_eta = ctk.CTkLabel(prog_frame, text="", font=ctk.CTkFont(size=11), text_color="#aaaaaa")
        self.lbl_eta.grid(row=0, column=1, padx=(10, 0))

    # ---------------------------------------------------------
    # UI COMPONENTS
    # ---------------------------------------------------------
    def setup_treeview(self, parent):
        style = ttk.Style()
        style.theme_use("default")
        style.configure("Treeview", background="#2b2b2b", foreground="white", rowheight=25, fieldbackground="#2b2b2b", borderwidth=0)
        style.map("Treeview", background=[("selected", "#1f538d")])
        style.configure("Treeview.Heading", background="#3b3b3b", foreground="white", relief="flat", font=("Arial", 10, "bold"))

        cols = ("Name", "Start Date", "End Date", "Submission", "Notification", "Registration")
        self.tree = ttk.Treeview(parent, columns=cols, show="headings")
        for c in cols:
            self.tree.heading(c, text=c)
            width = 300 if c == "Name" else 120
            self.tree.column(c, width=width, anchor="w" if c == "Name" else "center")
        
        scroll_y = ttk.Scrollbar(parent, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scroll_y.set)
        
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")

    def setup_chart(self, parent):
        self.fig = Figure(figsize=(5, 4), dpi=100, facecolor="#2b2b2b")
        self.ax = self.fig.add_subplot(111)
        self.ax.set_facecolor("#2b2b2b")
        self.ax.tick_params(colors="white")
        self.ax.pie([1], labels=["No Data"], colors=["#4b4b4b"], textprops={'color':"w"})
        self.canvas = FigureCanvasTkAgg(self.fig, master=parent)
        self.canvas.draw()
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")

    def setup_url_queue(self, parent):
        """URL Queue tab: shows all loaded URLs with live status updates."""
        cols = ("#", "URL", "Status", "Fields")
        self.url_tree = ttk.Treeview(parent, columns=cols, show="headings")
        col_cfg = {"#": (45, "center"), "URL": (490, "w"), "Status": (120, "center"), "Fields": (70, "center")}
        for c in cols:
            self.url_tree.heading(c, text=c)
            w, anchor = col_cfg[c]
            self.url_tree.column(c, width=w, anchor=anchor)

        self.url_tree.tag_configure("pending",    foreground="#aaaaaa")
        self.url_tree.tag_configure("processing", foreground="#fbbf24")
        self.url_tree.tag_configure("done",       foreground="#10b981")
        self.url_tree.tag_configure("error",      foreground="#ef4444")
        self.url_tree.tag_configure("skipped",    foreground="#6b7280")

        scroll_y = ttk.Scrollbar(parent, orient="vertical", command=self.url_tree.yview)
        self.url_tree.configure(yscrollcommand=scroll_y.set)
        self.url_tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")

    def populate_url_queue(self):
        """Rebuild the URL Queue treeview from current source_urls."""
        for iid in self.url_tree.get_children():
            self.url_tree.delete(iid)
        self.url_item_map.clear()

        urls = self.source_urls or []
        for i, url in enumerate(urls, 1):
            iid = self.url_tree.insert("", "end", values=(i, url, "⏳ Pending", "-"), tags=("pending",))
            self.url_item_map[url] = iid

        self.tabview.set("URL Queue")
        self.log_msg(f"URL Queue loaded: {len(urls)} URLs ready to process.", True)

    def setup_manual_review(self, parent):
        self.mr_scroll = ctk.CTkScrollableFrame(parent)
        self.mr_scroll.grid(row=0, column=0, sticky="nsew")
        self.mr_entries = [] # To keep track of dynamically generated entries
        self.lbl_mr_empty = ctk.CTkLabel(self.mr_scroll, text="Awaiting data...", text_color="gray")
        self.lbl_mr_empty.pack(pady=50)

    # ---------------------------------------------------------
    # SOURCE LOADING LOGIC
    # ---------------------------------------------------------
    def open_paste_modal(self):
        modal = ctk.CTkToplevel(self)
        modal.title("Paste Links")
        modal.geometry("500x400")
        modal.transient(self)
        modal.grab_set()

        lbl = ctk.CTkLabel(modal, text="Paste URLs here (one per line):", font=("Arial", 14, "bold"))
        lbl.pack(pady=10)

        textbox = ctk.CTkTextbox(modal, height=250)
        textbox.pack(fill="both", expand=True, padx=20, pady=10)

        def save_pasted():
            text = textbox.get("1.0", "end-1c")
            raw = [line.strip() for line in text.split("\n") if line.strip()]
            urls = self._filter_valid_urls(raw)
            if urls:
                self.source_urls = urls
                self.file_display.configure(text=f"Pasted: {len(urls)} URLs ({len(raw)-len(urls)} skipped)")
                self.after(0, self.populate_url_queue)
            modal.destroy()

        btn = ctk.CTkButton(modal, text="Save Links", command=save_pasted, fg_color="#28a745", hover_color="#218838")
        btn.pack(pady=10)

    def _filter_valid_urls(self, urls: list) -> list:
        """Drop entries that are clearly not valid URLs (e.g. '-', empty, no http)."""
        valid = []
        for u in urls:
            u = str(u).strip()
            if u and u not in ("-", "N/A", "n/a", "#", "nan") and u.lower().startswith("http"):
                valid.append(u)
            else:
                logging.warning(f"Skipping invalid URL entry: '{u}'")
        return valid

    def load_file(self):
        filepath = filedialog.askopenfilename(filetypes=[
            ("Supported Files", "*.xlsx *.xls *.csv *.txt"),
            ("Text Files", "*.txt"),
            ("Excel Files", "*.xlsx *.xls"),
            ("CSV Files", "*.csv")
        ])
        if not filepath:
            return

        ext = Path(filepath).suffix.lower()
        if ext == ".txt":
            with open(filepath, "r", encoding="utf-8") as f:
                raw = [line.strip() for line in f if line.strip()]
            self.source_urls = self._filter_valid_urls(raw)
            skipped = len(raw) - len(self.source_urls)
            self.file_display.configure(text=f"{Path(filepath).name}: {len(self.source_urls)} URLs" + (f" ({skipped} skipped)" if skipped else ""))
            self.after(0, self.populate_url_queue)
        elif ext == ".csv":
            df = pd.read_csv(filepath)
            col = next((c for c in df.columns if "url" in c.lower()), df.columns[0])
            raw = df[col].dropna().str.strip().tolist()
            self.source_urls = self._filter_valid_urls(raw)
            skipped = len(raw) - len(self.source_urls)
            self.file_display.configure(text=f"{Path(filepath).name}: {len(self.source_urls)} URLs" + (f" ({skipped} skipped)" if skipped else ""))
            self.after(0, self.populate_url_queue)
        elif ext in (".xlsx", ".xls"):
            self.open_excel_config_modal(filepath)

    def open_excel_config_modal(self, filepath):
        modal = ctk.CTkToplevel(self)
        modal.title("Excel Configuration")
        modal.geometry("400x350")
        modal.transient(self)
        modal.grab_set()
        
        try:
            xl = pd.ExcelFile(filepath)
            sheet_names = xl.sheet_names
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read Excel:\n{e}")
            modal.destroy()
            return

        lbl_sheet = ctk.CTkLabel(modal, text="1. Select Sheet:")
        lbl_sheet.pack(pady=(20, 5))
        
        sheet_var = ctk.StringVar(value=sheet_names[0])
        opt_sheet = ctk.CTkOptionMenu(modal, values=sheet_names, variable=sheet_var)
        opt_sheet.pack(pady=5)

        lbl_col = ctk.CTkLabel(modal, text="2. Select Column:")
        lbl_col.pack(pady=(20, 5))
        
        col_var = ctk.StringVar()
        opt_col = ctk.CTkOptionMenu(modal, values=["- Select Sheet First -"], variable=col_var)
        opt_col.pack(pady=5)

        def update_columns(*args):
            df = pd.read_excel(filepath, sheet_name=sheet_var.get(), nrows=0)
            cols = list(df.columns)
            opt_col.configure(values=[str(c) for c in cols])
            default = next((str(c) for c in cols if 'url' in str(c).lower()), str(cols[0]) if cols else "")
            col_var.set(default)
        
        sheet_var.trace_add("write", update_columns)
        update_columns()

        def apply_config():
            if not col_var.get():
                return
            df = pd.read_excel(filepath, sheet_name=sheet_var.get())
            raw = df[col_var.get()].dropna().str.strip().tolist()
            self.source_urls = self._filter_valid_urls(raw)
            skipped = len(raw) - len(self.source_urls)
            label = f"{Path(filepath).name}: {len(self.source_urls)} URLs"
            if skipped:
                label += f" ({skipped} invalid skipped)"
            self.file_display.configure(text=label)
            self.after(0, self.populate_url_queue)
            modal.destroy()

        btn = ctk.CTkButton(modal, text="Load URLs", command=apply_config)
        btn.pack(pady=30)

    # ---------------------------------------------------------
    # PIPELINE EXECUTION
    # ---------------------------------------------------------
    def start_pipeline(self):
        if self.is_running: return
        self.is_running = True
        self._start_time = time.time()
        self.stop_event.clear()
        self.pause_event.clear()
        self.btn_run.configure(state="disabled")
        self.btn_paste.configure(state="disabled")
        self.btn_load_file.configure(state="disabled")
        self.btn_pause.configure(state="normal", text="⏸ Pause")
        self.btn_stop.configure(state="normal")
        self.btn_export.configure(state="disabled")
        self.progress_bar.set(0)
        self.lbl_processed.configure(text="Processed: 0 / 0")
        self.lbl_eta.configure(text="")

        for item in self.tree.get_children():
            self.tree.delete(item)

        # Reset URL queue statuses to Pending
        for iid in self.url_item_map.values():
            vals = list(self.url_tree.item(iid, "values"))
            vals[2] = "⏳ Pending"
            vals[3] = "-"
            self.url_tree.item(iid, values=vals, tags=("pending",))

        self.update_pie_chart({})
        self.log_msg("--- PIPELINE STARTED ---", True)
        self.tabview.set("URL Queue")

        self.worker_thread = threading.Thread(target=self.run_worker, daemon=True)
        self.worker_thread.start()

    def pause_pipeline(self):
        if self.pause_event.is_set():
            self.pause_event.clear()
            self.btn_pause.configure(text="⏸ Pause")
            self.log_msg("Pipeline Resumed.", True)
        else:
            self.pause_event.set()
            self.btn_pause.configure(text="▶ Resume")
            self.log_msg("Pipeline Paused...", True)

    def stop_pipeline(self):
        self.stop_event.set()
        self.pause_event.clear()
        self.log_msg("Waiting for current URL to finish, shutting down...", True)
        self.btn_pause.configure(state="disabled")
        self.btn_stop.configure(state="disabled")

    def run_worker(self):
        mode_map = {"All (Dates + Topics)": "all", "Dates Only": "dates_only", "Topics Only": "topics_only"}
        selected_mode = mode_map.get(self.mode_var.get(), "all")
        
        try:
            run_pipeline(
                fuente_urls=None, 
                url_list=self.source_urls, # Pass explicit urls or None (default db)
                progress_callback=self.on_pipeline_progress,
                stop_event=self.stop_event,
                pause_event=self.pause_event,
                auto_export=False, # GUI handles export
                extraction_mode=selected_mode
            )
        except Exception as e:
            logging.error(f"Fatal pipeline error: {e}", exc_info=True)
        finally:
            # Safety net: if main() finished or crashed without sending done=True
            if self.is_running:
                logging.info("Worker thread ended — sending fallback completion signal.")
                # By not providing 'records' or 'change_report', we avoid overwriting with old values
                self.update_queue.put({
                    "type": "progress",
                    "data": {
                        "done": True
                    }
                })

    def on_pipeline_progress(self, info: dict):
        self.update_queue.put({"type": "progress", "data": info})

    # ---------------------------------------------------------
    # QUEUE CONSUMER
    # ---------------------------------------------------------
    def poll_queues(self):
        while not self.log_queue.empty():
            try:
                msg = self.log_queue.get_nowait()
                self.log_msg(msg)
            except queue.Empty:
                break

        while not self.update_queue.empty():
            try:
                msg = self.update_queue.get_nowait()
                if msg["type"] == "progress":
                    data = msg.get("data", {})
                    if data.get("done"):
                        self.handle_pipeline_done(data)
                    else:
                        self.handle_pipeline_step(data)
            except queue.Empty:
                break
                
        self.after(100, self.poll_queues)

    def handle_pipeline_step(self, data):
        i = data.get("index", 1)
        tot = data.get("total", 1)
        record = data.get("record", {})
        url = record.get("url", "")
        method = data.get("method", "Unknown")
        fields = record.get("fields_found", "?")
        is_error = method.lower() == "error"

        c_name = record.get("conference_name")
        c_name_str = str(c_name)[:40] if c_name else "Unknown"

        self.tree.insert("", "end", values=(
            c_name_str,
            record.get("fecha_inicio", "N/A"),
            record.get("fecha_fin", "N/A"),
            record.get("envio_trabajo", "N/A"),
            record.get("notificacion_aceptacion", "N/A"),
            record.get("inscripcion", "N/A")
        ))
        self.tree.yview_moveto(1.0)

        # Update URL Queue row status
        if url in self.url_item_map:
            iid = self.url_item_map[url]
            tag = "error" if is_error else "done"
            status = "❌ Error" if is_error else "✅ Done"
            vals = list(self.url_tree.item(iid, "values"))
            vals[2] = status
            vals[3] = fields
            self.url_tree.item(iid, values=vals, tags=(tag,))
            self.url_tree.see(iid)

        self.progress_bar.set(i / max(1, tot))
        self.lbl_processed.configure(text=f"Processed: {i} / {tot}")
        self.lbl_errors.configure(text=f"Errors: {data.get('stats', {}).get('error', 0)}")
        self.update_pie_chart(data.get("stats", {}))

        # ETA
        elapsed = time.time() - self._start_time
        if i > 0:
            avg_per = elapsed / i
            remaining = avg_per * max(0, tot - i)
            self.lbl_eta.configure(text=f"⏱ {self._fmt_time(elapsed)} elapsed · ~{self._fmt_time(remaining)} left")

    def handle_pipeline_done(self, data):
        self.is_running = False
        self.btn_run.configure(state="normal")
        self.btn_paste.configure(state="normal")
        self.btn_load_file.configure(state="normal")
        self.btn_pause.configure(state="disabled", text="⏸ Pause")
        self.btn_stop.configure(state="disabled")

        if "records" in data:
            self.final_records = data["records"]
        if "change_report" in data:
            self.final_change_report = data["change_report"]
        stats = data.get("stats", {})

        # Final elapsed time
        elapsed = time.time() - self._start_time
        self.lbl_eta.configure(text=f"✅ Finished in {self._fmt_time(elapsed)}")
        self.progress_bar.set(1.0)

        self.log_msg("--- PIPELINE FINISHED ---", True)
        self.update_pie_chart(stats)

        # Completion summary in log
        total = len(self.final_records)
        complete = sum(1 for r in self.final_records if r.get("fields_found") == "5/5")
        errors = stats.get("error", 0)
        self.log_msg(
            f"Summary: {total} processed · {complete} complete (5/5) · {errors} errors · {self._fmt_time(elapsed)} total",
            True
        )

        self.populate_manual_review()

    # ---------------------------------------------------------
    # MANUAL REVIEW UI
    # ---------------------------------------------------------
    def populate_manual_review(self):
        for widget in self.mr_scroll.winfo_children():
            widget.destroy()
        
        self.mr_entries = []
        needs_review = []

        if not self.final_records:
             lbl = ctk.CTkLabel(self.mr_scroll, text="No records extracted.", text_color="gray")
             lbl.pack(pady=50)
             self.btn_export.configure(state="normal")
             return

        # Identify changes
        changes_by_url = {}
        if self.final_change_report:
            for c in self.final_change_report.changes:
                if c.url not in changes_by_url:
                    changes_by_url[c.url] = {}
                changes_by_url[c.url][c.field] = c

        for i, rec in enumerate(self.final_records):
            ff = rec.get("fields_found", "0/5")
            has_changes = rec["url"] in changes_by_url
            # Needs manual review if not 5/5 OR if it has changed values
            if ff != "5/5" or has_changes:
                needs_review.append((i, rec, changes_by_url.get(rec["url"], {})))

        if not needs_review:
            lbl = ctk.CTkLabel(self.mr_scroll, text="All records have 5/5 fields and no unexpected changes.\nReady to Export!", text_color="#10b981", font=("Arial", 16, "bold"))
            lbl.pack(pady=50)
            self.btn_export.configure(state="normal")
            return

        self.tabview.set("Manual Review")
        title = ctk.CTkLabel(self.mr_scroll, text=f"Review required for {len(needs_review)} Conferences", font=("Arial", 16, "bold"))
        title.pack(pady=(10, 20))

        # Build grid headers
        header_fr = ctk.CTkFrame(self.mr_scroll, fg_color="transparent")
        header_fr.pack(fill="x", padx=10)
        ctk.CTkLabel(header_fr, text="URL / Conference").grid(row=0, column=0, padx=5, sticky="w")
        for i, k in enumerate(DATE_KEYS, 1):
             ctk.CTkLabel(header_fr, text=k.replace("_", " ").title()).grid(row=0, column=i, padx=5)

        for rec_idx, rec, url_changes in needs_review:
            row_fr = ctk.CTkFrame(self.mr_scroll)
            row_fr.pack(fill="x", pady=5, padx=10)
            
            lbl_name = ctk.CTkLabel(row_fr, text=rec.get("url", "")[:40] + "...", width=300, anchor="w", justify="left")
            lbl_name.grid(row=0, column=0, padx=5, pady=5, sticky="w")
            
            row_entries = {}
            for i, k in enumerate(DATE_KEYS, 1):
                val = rec.get(k) or ""
                # Orange if changed recently from database, Red if empty
                color = "white"
                border = ["#979da2", "#565b5e"] # default
                has_changed = k in url_changes
                
                if has_changed:
                    color = "#f97316" # orange
                    border = "#f97316"
                elif not val:
                    border = "#ef4444" # red warning
                    
                entry = ctk.CTkEntry(row_fr, width=110, text_color=color)
                if border != ["#979da2", "#565b5e"]:
                    entry.configure(border_color=border)
                
                entry.insert(0, val)
                entry.grid(row=0, column=i, padx=5, pady=5)
                row_entries[k] = entry
                
                # Show tooltip or label for changes
                if has_changed:
                     old_v = url_changes[k].old_value
                     ctk.CTkLabel(row_fr, text=f"old: {old_v}", text_color="#f97316", font=("Arial", 9)).grid(row=1, column=i)
            
            self.mr_entries.append({"idx": rec_idx, "entries": row_entries})

        btn_save = ctk.CTkButton(self.mr_scroll, text="Save Manual Edits", fg_color="#28a745", hover_color="#218838", command=self.save_manual_edits)
        btn_save.pack(pady=20)
        
        # Unlock export early if they want to ignore
        self.btn_export.configure(state="normal")

    def save_manual_edits(self):
        for item in self.mr_entries:
            rec_idx = item["idx"]
            for k in DATE_KEYS:
                val = item["entries"][k].get().strip()
                self.final_records[rec_idx][k] = val if val else None
                
            # Re-calculate fields found 
            filled = sum(1 for k in DATE_KEYS if self.final_records[rec_idx].get(k) is not None)
            self.final_records[rec_idx]["fields_found"] = f"{filled}/5"
            self.final_records[rec_idx]["notas"] += " | Manually Edited"

        messagebox.showinfo("Saved", "Manual edits saved to memory. You can now Export Excel.")

    # ---------------------------------------------------------
    # EXPORT Excel
    # ---------------------------------------------------------
    def export_excel(self):
        default_name = "reporte_conferencias.xlsx"
        filepath = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            initialfile=default_name,
            filetypes=[("Excel files", "*.xlsx")],
            title="Export Excel Report"
        )
        if filepath:
            try:
                if self.final_change_report is None:
                    from utils.change_detector import ChangeReport
                    self.final_change_report = ChangeReport()
                write_excel_report(self.final_records, self.final_change_report, output_path=Path(filepath))
                messagebox.showinfo("Success", f"Report successfully saved to:\n{filepath}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save Excel:\n{e}")

    # ---------------------------------------------------------
    # UTILS
    # ---------------------------------------------------------
    # ---------------------------------------------------------
    # LOG UTILITIES
    # ---------------------------------------------------------
    def copy_log(self):
        content = self.log_textbox.get("1.0", "end-1c")
        self.clipboard_clear()
        self.clipboard_append(content)
        self.log_msg("Log copied to clipboard.", True)

    def clear_log(self):
        self.log_textbox.configure(state="normal")
        self.log_textbox.delete("1.0", "end")
        self.log_textbox.configure(state="disabled")

    def _fmt_time(self, seconds: float) -> str:
        """Format seconds → mm:ss or h mm:ss."""
        s = int(seconds)
        if s < 3600:
            return f"{s // 60:02d}:{s % 60:02d}"
        return f"{s // 3600}h {(s % 3600) // 60:02d}m"

    def update_pie_chart(self, stats: dict):
        self.ax.clear()
        groups = {
            "Cache": sum(v for k,v in stats.items() if k == "cache"),
            "Regex Only": sum(v for k,v in stats.items() if "regex" in k and "llm" not in k),
            "LLM": sum(v for k,v in stats.items() if "llm" in k),
            "Image Content": stats.get("image-content", 0),
            "Errors": stats.get("error", 0),
        }
        labels = []
        sizes = []
        colors = []
        color_map = {"Cache": "#10b981", "Regex Only": "#3b82f6", "LLM": "#8b5cf6", "Image Content": "#f59e0b", "Errors": "#ef4444"}
        
        for k, v in groups.items():
            if v > 0:
                labels.append(k)
                sizes.append(v)
                colors.append(color_map[k])
                
        if not sizes:
            self.ax.pie([1], labels=["No Data"], colors=["#4b4b4b"], textprops={'color':"w"})
        else:
            self.ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90, textprops={'color':"white"})
            self.ax.axis('equal')
            
        self.canvas.draw()

    def log_msg(self, msg: str, is_system: bool = False):
        self.log_textbox.configure(state="normal")
        term = f">>> {msg}\n" if is_system else f"{msg}\n"
        self.log_textbox.insert("end", term)
        self.log_textbox.see("end")
        self.log_textbox.configure(state="disabled")

if __name__ == "__main__":
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")
    app = ConferenceScraperApp()
    app.mainloop()
