import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import os
import re
import copy

# ==================================================
# ================= DATA ENGINE ====================
# ==================================================

SQL_RESERVED = {
    "select", "from", "where", "group", "order", "table",
    "insert", "update", "delete", "join", "limit"
}

def standardize_column_names(df):
    cols = []
    for col in df.columns:
        c = str(col).strip().lower().replace(" ", "_")
        c = re.sub(r"[^\w_]", "", c)
        if c in SQL_RESERVED:
            c = f"{c}_col"
        cols.append(c)
    df.columns = cols
    return df

def clean_dataset(df):
    df = standardize_column_names(df.copy())
    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ==================================================
# ================== GUI APP =======================
# ==================================================
class SmartDataAnalyser(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("Smart Data Analyser")
        self.geometry("1500x850")

        # ---------------- DATA STATE ----------------
        self.datasets = {}
        self.paths = []
        self.current_df = None
        self.preview_df = None
        self.aggregated_df = None
        self.current_name = None

        # ---------------- HISTORY / UNDO ----------------
        self.history = []          # text labels
        self.undo_stack = []       # dataframe snapshots

        # ---------------- UI STATE ----------------
        self.mode = tk.StringVar(value="Mode 1")
        self.chart_type = tk.StringVar(value="Bar")

        self.build_ui()

    # -------------------------------------------------
    # ---------------- CLEAR DATA ---------------------
    # -------------------------------------------------
    def clear_data(self):
        self.datasets.clear()
        self.paths = []
        self.current_df = None
        self.preview_df = None
        self.aggregated_df = None
        self.current_name = None

        # Clear UI
        self.dataset_combo.set("")
        self.dataset_combo["values"] = []

        self.table.delete(*self.table.get_children())
        self.groupby_list.delete(0, tk.END)
        self.value_col.set("")
        self.info.config(text="")

        for w in self.chart_frame.winfo_children():
            w.destroy()

        self.history_list.delete(0, tk.END)
        self.history.clear()
        self.undo_stack.clear()

        self.file_label.config(text="No file selected")


    # ==================================================
    # ================= UI LAYOUT ======================
    # ==================================================

    def build_ui(self):

        # ---------------- TOP BAR ----------------
        top = tk.Frame(self)
        top.grid(row=0, column=0, columnspan=2, sticky="w", padx=10, pady=5)

        tk.Radiobutton(top, text="Mode 1 (Single Dataset)",
                       variable=self.mode, value="Mode 1").pack(side="left")
        tk.Radiobutton(top, text="Mode 2 (Multiple Datasets)",
                       variable=self.mode, value="Mode 2").pack(side="left", padx=10)

        tk.Button(top, text="Browse", command=self.browse_files).pack(side="left", padx=5)
        tk.Button(top, text="Read", command=self.read_files).pack(side="left")
        tk.Button(top, text="Clean Data", command=self.clean_current).pack(side="left", padx=5)
        tk.Button(top, text="Undo", command=self.undo_last).pack(side="left", padx=5)
        tk.Button(top, text="Export", command=self.export_router).pack(side="left", padx=5)
        tk.Button(top, text="Clear Data", command=self.clear_data).pack(side="left", padx=5)

        self.file_label = tk.Label(top, text="No file selected")
        self.file_label.pack(side="left", padx=10)

        # ---------------- LEFT PANEL ----------------
        left = tk.Frame(self)
        left.grid(row=1, column=0, sticky="ns", padx=10)

        self.dataset_combo = ttk.Combobox(left, state="readonly", width=28)
        self.dataset_combo.pack(fill="x")
        self.dataset_combo.bind("<<ComboboxSelected>>", self.switch_dataset)

        ctrl = tk.Frame(left)
        ctrl.pack(fill="x", pady=5)

        self.limit_var = tk.StringVar(value="All")
        ttk.Combobox(ctrl, textvariable=self.limit_var,
                     values=["All", "Top 5", "Top 10", "Top 50"],
                     width=8, state="readonly").pack(side="left")

        self.sort_var = tk.StringVar(value="Descending")
        ttk.Combobox(ctrl, textvariable=self.sort_var,
                     values=["Ascending", "Descending"],
                     width=12, state="readonly").pack(side="left", padx=5)

        tk.Button(ctrl, text="Apply", command=self.refresh_preview).pack(side="left")

        search_frame = tk.Frame(left)
        search_frame.pack(fill="x", pady=5)

        self.search_entry = tk.Entry(search_frame)
        self.search_entry.pack(side="left", fill="x", expand=True)
        tk.Button(search_frame, text="Search",
                  command=self.search_data).pack(side="left", padx=5)

        agg = tk.LabelFrame(left, text="Aggregation & Insights")
        agg.pack(fill="x", pady=5)

        tk.Label(agg, text="Group By").pack(anchor="w")
        self.groupby_list = tk.Listbox(agg, selectmode=tk.MULTIPLE, height=6)
        self.groupby_list.pack(fill="x")

        tk.Label(agg, text="Value Column").pack(anchor="w", pady=(5, 0))
        self.value_col = ttk.Combobox(agg)
        self.value_col.pack(fill="x")

        tk.Label(agg, text="Aggregation").pack(anchor="w", pady=(5, 0))
        self.agg_func = ttk.Combobox(
            agg, values=["count", "distinct", "sum", "mean", "min", "max"]
        )
        self.agg_func.pack(fill="x")

        tk.Button(agg, text="Preview Aggregation",
                  command=self.run_aggregation).pack(fill="x", pady=5)

        hist = tk.LabelFrame(left, text="History")
        hist.pack(fill="x", pady=5)

        self.history_list = tk.Listbox(hist, height=6)
        self.history_list.pack(fill="x")

        # ---------------- RIGHT PANEL ----------------
        right = tk.Frame(self)
        right.grid(row=1, column=1, sticky="nsew", padx=10)

        info_frame = tk.Frame(right)
        info_frame.pack(fill="x")
        self.info = tk.Label(info_frame, text="Rows: 0 | Columns: 0")
        self.info.pack(side="right")

        table_frame = tk.Frame(right)
        table_frame.pack(fill="both", expand=True)

        self.table = ttk.Treeview(table_frame, show="headings")
        self.table.pack(side="left", fill="both", expand=True)

        scroll = ttk.Scrollbar(table_frame, orient="vertical",
                              command=self.table.yview)
        scroll.pack(side="right", fill="y")
        self.table.configure(yscrollcommand=scroll.set)

        chart_box = tk.LabelFrame(right, text="Chart Box")
        chart_box.pack(fill="x", pady=5)

        ttk.Combobox(chart_box, textvariable=self.chart_type,
                     values=["Bar", "Line", "Pie"],
                     state="readonly").pack(fill="x")

        btns = tk.Frame(chart_box)
        btns.pack(fill="x", pady=5)

        tk.Button(btns, text="Preview Chart",
                  command=self.draw_chart).pack(side="left", padx=5)
        tk.Button(btns, text="Export Chart",
                  command=self.export_chart).pack(side="left")

        self.chart_frame = tk.Frame(chart_box, height=220)
        self.chart_frame.pack(fill="both")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(1, weight=1)

    # ==================================================
    # ================= HISTORY / UNDO =================
    # ==================================================

    def push_history(self, label):
        self.history.append(label)
        self.history_list.insert(tk.END, label)
        self.undo_stack.append(copy.deepcopy(self.current_df))

    def undo_last(self):
        if not self.undo_stack:
            return
        self.undo_stack.pop()
        self.history.pop()
        self.history_list.delete(tk.END)

        if self.undo_stack:
            self.current_df = copy.deepcopy(self.undo_stack[-1])
            self.preview_df = self.current_df.copy()
            self.aggregated_df = None
            self.refresh_preview(clear_chart=True)

    # ==================================================
    # ================= CORE LOGIC =====================
    # ==================================================

    def browse_files(self):
        self.paths = filedialog.askopenfilenames(
            filetypes=[("CSV", "*.csv"), ("Excel", "*.xlsx")]
        )
        if self.paths:
            self.file_label.config(text=f"{len(self.paths)} file(s) selected")
            self.push_history("Browse")

    def read_files(self):
        self.datasets.clear()
        for path in self.paths:
            name = os.path.splitext(os.path.basename(path))[0]
            df = pd.read_csv(path) if path.endswith(".csv") else pd.read_excel(path)
            self.datasets[name] = df

        self.dataset_combo["values"] = list(self.datasets.keys())
        if self.datasets:
            self.dataset_combo.current(0)
            self.switch_dataset()
            self.push_history("Read")

    def switch_dataset(self, event=None):
        self.current_name = self.dataset_combo.get()
        self.current_df = self.datasets[self.current_name].copy()
        self.preview_df = self.current_df.copy()
        self.aggregated_df = None

        self.groupby_list.delete(0, tk.END)
        for c in self.current_df.columns:
            self.groupby_list.insert(tk.END, c)

        self.value_col["values"] = list(self.current_df.columns)
        self.refresh_preview()

    def refresh_preview(self, clear_chart=False):
        if self.preview_df is None:
            return

        df = self.preview_df.copy()

        numeric_cols = df.select_dtypes(include=np.number).columns
        sort_col = numeric_cols[-1] if len(numeric_cols) else df.columns[0]

        df = df.sort_values(
            by=sort_col,
            ascending=self.sort_var.get() == "Ascending"
        )

        if self.limit_var.get() != "All":
            df = df.head(int(self.limit_var.get().split()[1]))

        self.show_table(df)

        if clear_chart:
            for w in self.chart_frame.winfo_children():
                w.destroy()

    def show_table(self, df):
        self.table.delete(*self.table.get_children())
        self.table["columns"] = list(df.columns)

        for c in df.columns:
            self.table.heading(c, text=c)
            self.table.column(c, width=120)

        for _, r in df.iterrows():
            self.table.insert("", "end", values=list(r))

        self.info.config(text=f"Rows: {len(df)} | Columns: {len(df.columns)}")

    def search_data(self):
        q = self.search_entry.get().lower()
        if not q:
            self.refresh_preview()
            return

        mask = self.preview_df.astype(str).apply(
            lambda r: r.str.lower().str.contains(q).any(), axis=1
        )
        self.show_table(self.preview_df[mask])

    # ✅ FIXED CLEAN METHOD
    def clean_current(self):
        if self.current_df is None:
            return

        # Save state for undo + history
        self.push_history("Clean")

        # Clean the data
        self.current_df = clean_dataset(self.current_df)
        self.preview_df = self.current_df.copy()
        self.aggregated_df = None

        # Update dataset store
        if self.current_name:
            self.datasets[self.current_name] = self.current_df

        # Refresh Group By list
        self.groupby_list.delete(0, tk.END)
        for c in self.current_df.columns:
            self.groupby_list.insert(tk.END, c)

        # Refresh Value Column list
        self.value_col["values"] = list(self.current_df.columns)
        self.value_col.set("")

        # Refresh preview and clear chart
        self.refresh_preview(clear_chart=True)


    # ==================================================
    # ================= AGGREGATION ====================
    # ==================================================

    def run_aggregation(self):
        df = self.current_df.copy()
        group_cols = [self.groupby_list.get(i)
                      for i in self.groupby_list.curselection()]
        value_col = self.value_col.get().strip()
        func = self.agg_func.get().strip()

        if not group_cols and value_col and func:
            df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
            result = getattr(df[value_col], func)()
            self.aggregated_df = pd.DataFrame({value_col: [result]})

        elif group_cols and func in ["count", "distinct"] and not value_col:
            self.aggregated_df = df.groupby(group_cols).size().reset_index(name=func)

        else:
            if func in ["sum", "mean", "min", "max"]:
                df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
            self.aggregated_df = df.groupby(group_cols)[value_col].agg(func).reset_index()

        self.preview_df = self.aggregated_df.copy()
        self.show_table(self.preview_df)
        self.push_history("Aggregation")

    # ==================================================
    # ================= CHART ==========================
    # ==================================================

    def draw_chart(self):
        if self.preview_df is None or self.preview_df.shape[1] < 2:
            return

        for w in self.chart_frame.winfo_children():
            w.destroy()

        fig, ax = plt.subplots(figsize=(6, 3))
        x = self.preview_df.iloc[:, 0].astype(str)
        y = self.preview_df.iloc[:, 1]

        if self.chart_type.get() == "Pie":
            ax.pie(y, labels=x, autopct="%1.1f%%")
        elif self.chart_type.get() == "Line":
            ax.plot(x, y, marker="o")
        else:
            ax.bar(x, y)

        fig.tight_layout()
        canvas = FigureCanvasTkAgg(fig, self.chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both")

    def export_chart(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG Image", "*.png")]
        )
        if not path:
            return
        self.draw_chart()
        plt.savefig(path)
        messagebox.showinfo("Chart Exported", f"Saved to:\n{path}")
    # ==================================================
    # ================= EXPORT =========================
    # ==================================================

    def export_router(self):
        if self.mode.get() == "Mode 1":
            self.export_mode1()
        else:
            self.export_mode2()


    # ---------- MODE 1 EXPORT ----------
    def export_mode1(self):
        if self.preview_df is None or self.preview_df.empty:
            messagebox.showwarning("Export", "No data to export")
            return

        base_dir = os.path.dirname(self.paths[0]) if self.paths else os.getcwd()

        path = filedialog.asksaveasfilename(
            initialdir=base_dir,
            initialfile=f"{self.current_name}_export",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("Excel", "*.xlsx")]
        )
        if not path:
            return

        if path.endswith(".xlsx"):
            self.preview_df.to_excel(path, index=False)
        else:
            self.preview_df.to_csv(path, index=False)

        messagebox.showinfo("Export", f"Saved to:\n{path}")


    # ---------- MODE 2 EXPORT ----------
        # ---------- MODE 2 EXPORT ----------
    # ---------- MODE 2 EXPORT ----------
    def export_mode2(self):
        if not self.datasets:
            messagebox.showwarning("Export", "No datasets loaded")
            return

        dialog = tk.Toplevel(self)
        dialog.title("Mode 2 Export Options")
        dialog.geometry("350x250")
        dialog.resizable(False, False)

        choice = tk.StringVar(value="append")

        ttk.Radiobutton(dialog, text="Append (Row-wise)",
                        variable=choice, value="append").pack(anchor="w", padx=10, pady=5)

        ttk.Radiobutton(dialog, text="Master Sheet (Multiple Sheets)",
                        variable=choice, value="master").pack(anchor="w", padx=10, pady=5)

        ttk.Radiobutton(dialog, text="Merge (Join)",
                        variable=choice, value="merge").pack(anchor="w", padx=10, pady=5)

        def proceed():
            dialog.destroy()
            if choice.get() == "append":
                self.export_append()
            elif choice.get() == "master":
                self.export_master()
            else:
                self.open_merge_dialog()

        ttk.Button(dialog, text="OK", command=proceed).pack(pady=20)


    def export_append(self):
        dfs = [clean_dataset(df.copy()) for df in self.datasets.values()]
        combined = pd.concat(dfs, ignore_index=True)
        self._final_export(combined, "append")

    def export_append(self):
        try:
            if not self.paths:
                raise Exception("Please use Browse and Read first.")

            base_dir = os.path.dirname(self.paths[0])
            file_path = os.path.join(base_dir, "append_export.csv")

            combined = pd.concat(
                [clean_dataset(df.copy()) for df in self.datasets.values()],
                ignore_index=True
            )

            combined.to_csv(file_path, index=False)

            messagebox.showinfo(
                "Export Successful",
                f"Append CSV exported successfully.\n\nLocation:\n{file_path}"
            )

        except Exception as e:
            messagebox.showerror("Export Failed", str(e))



    def export_master(self):
        try:
            if not self.paths:
                raise Exception("Please use Browse and Read first.")

            base_dir = os.path.dirname(self.paths[0])
            file_path = os.path.join(base_dir, "master_export.csv")

            combined = pd.concat(
                [clean_dataset(df.copy()) for df in self.datasets.values()],
                ignore_index=True
            )

            combined.to_csv(file_path, index=False)

            messagebox.showinfo(
                "Export Successful",
                f"Master CSV exported successfully.\n\nLocation:\n{file_path}"
            )

        except Exception as e:
            messagebox.showerror("Export Failed", str(e))


    def open_merge_dialog(self):
        dialog = tk.Toplevel(self)
        dialog.title("Merge Settings")
        dialog.geometry("350x260")
        dialog.resizable(False, False)

        tk.Label(dialog, text="Join Type").pack(anchor="w", padx=10, pady=(10, 0))
        join_type = ttk.Combobox(
            dialog,
            values=["inner", "left", "right", "outer"],
            state="readonly"
        )
        join_type.pack(fill="x", padx=10)
        join_type.current(0)

        tk.Label(dialog, text="Key Column").pack(anchor="w", padx=10, pady=(10, 0))
        all_cols = set()
        for df in self.datasets.values():
            all_cols.update(df.columns)

        key_col = ttk.Combobox(dialog, values=list(all_cols))
        key_col.pack(fill="x", padx=10)

        tk.Label(dialog, text="(Optional) Manual Key Column").pack(anchor="w", padx=10, pady=(10, 0))
        manual_key = tk.Entry(dialog)
        manual_key.pack(fill="x", padx=10)

        def proceed_merge():
            col = manual_key.get().strip() or key_col.get().strip()
            if not col:
                messagebox.showwarning("Merge", "Please select or enter a key column")
                return

            jt = join_type.get()

            # 🔥 PROOF OF CLICK (YOU MUST SEE THIS)
            messagebox.showinfo(
                "DEBUG",
                f"Merge clicked\nJoin: {jt}\nKey: {col}"
            )

            dialog.destroy()

            # 🔥 SAFE CALL (Tkinter-approved)
            self.after(0, lambda: self.perform_merge(jt, col))

        tk.Button(dialog, text="Export", command=proceed_merge).pack(pady=20)


    def perform_merge(self, join_type, key_col):
        try:
            base_dir = os.path.dirname(self.paths[0])
            file_path = os.path.join(base_dir, "merged_export.csv")

            merged_df = None
            key_col_clean = key_col.strip().lower().replace(" ", "_")

            for df in self.datasets.values():
                df = clean_dataset(df.copy())

                if key_col_clean not in df.columns:
                    raise Exception(
                        f"Key '{key_col}' not found.\nAvailable columns:\n{', '.join(df.columns)}"
                    )

                if merged_df is None:
                    merged_df = df
                else:
                    merged_df = pd.merge(
                        merged_df,
                        df,
                        how=join_type,
                        on=key_col_clean
                    )

            if merged_df is None:
                merged_df = pd.DataFrame()

            merged_df.to_csv(file_path, index=False)

            messagebox.showinfo(
                "Export Successful",
                f"Merged CSV saved successfully.\n\nLocation:\n{file_path}"
            )

        except Exception as e:
            messagebox.showerror("Merge Failed", str(e))

    def export_merge_csv(self, df):
        try:
            base_dir = os.path.dirname(self.paths[0])
            full_path = os.path.join(base_dir, "merged_export.csv")

            # Always create the file
            if df is None:
                df = pd.DataFrame()

            df.to_csv(full_path, index=False)

            messagebox.showinfo(
                "Export Successful",
                f"Merged CSV file saved successfully.\n\nLocation:\n{full_path}"
            )

        except Exception as e:
            messagebox.showerror("Export Failed", str(e))

    def _final_export(self, df, label):
        try:
            if not self.paths:
                raise Exception("No import path found. Please use Browse + Read first.")

            base_dir = os.path.dirname(self.paths[0])

            if label == "append":
                file_path = os.path.join(base_dir, "append_export.csv")
                df.to_csv(file_path, index=False)

            elif label == "merged":
                file_path = os.path.join(base_dir, "merged_export.csv")
                df.to_csv(file_path, index=False)

            else:
                raise Exception(f"Unknown export type: {label}")

            if not os.path.exists(file_path):
                raise Exception("File was not created.")

            messagebox.showinfo(
                "Export Successful",
                f"File saved successfully.\n\nLocation:\n{file_path}"
            )

        except Exception as e:
            messagebox.showerror(
                "Export Failed",
                str(e)
            )


# ==================================================
# ================== RUN ===========================
# ==================================================
if __name__ == "__main__":
    SmartDataAnalyser().mainloop()
