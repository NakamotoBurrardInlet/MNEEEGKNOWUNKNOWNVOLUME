import time
import json
import sqlite3
from pathlib import Path
from typing import Dict, Any

class AcademicTelemetryProcessor:
    """
    High-impact academic telemetry processor implementing continuous ingestion, 
    binary/hexadecimal stream formatting, analytical metric extraction, and 
    persistent SQLite relational storage.
    """
    def __init__(
        self, 
        ledger_path: str = "omega_silent_knowing_ledger.json",
        db_path: str = "omega_telemetry_nexus.db"
    ):
        self.ledger_path = Path(ledger_path)
        self.db_path = Path(db_path)
        
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Establishes the relational SQLite schema for archival and querying."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                utc_time TEXT,
                node_signature TEXT,
                pipeline_mode TEXT,
                mean_aggregate REAL,
                std_aggregate REAL,
                binary_frame TEXT,
                hex_frame TEXT,
                raw_payload TEXT
            )
        """)
        self.conn.commit()

    def process_stream(self) -> None:
        """
        Executes the continuous main-looping processing pipeline, tailing the 
        ledger file, formatting binary streams, and committing records to the database.
        """
        print(f"[ACADEMIC NEXUS] Initializing Professorial Telemetry Processor...")
        print(f"[ACADEMIC NEXUS] Tailing source: {self.ledger_path}")
        print(f"[ACADEMIC NEXUS] Archival target: {self.db_path}\n")

        # Ensure source ledger exists before streaming
        while not self.ledger_path.exists():
            print("[WAIT] Awaiting active emitter ledger stream...")
            time.sleep(1.0)

        with open(self.ledger_path, "r", encoding="utf-8") as f:
            # Seek to end to process live incoming stream
            f.seek(0, 2)
            
            try:
                while True:
                    line = f.readline()
                    if not line:
                        time.sleep(0.05)
                        continue
                    
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record: Dict[str, Any] = json.loads(line)
                        self._analyze_and_persist(record)
                    except json.JSONDecodeError:
                        continue
                        
            except KeyboardInterrupt:
                print("\n[ACADEMIC NEXUS] Terminating processing session gracefully.")
            finally:
                self.conn.close()

    def _analyze_and_persist(self, record: Dict[str, Any]) -> None:
        """Performs analytical aggregation, binary encoding, and database commitment."""
        timestamp = record.get("timestamp", time.time_ns())
        utc_time = record.get("constellation_utc", "UNKNOWN_UTC")
        node_sig = record.get("node_signature", "UNKNOWN_NODE")
        pipeline = record.get("pipeline", "UNKNOWN_PIPELINE")
        
        metrics = record.get("grey_matter_metrics", {})
        means = metrics.get("means", [0.0])
        stds = metrics.get("stds", [0.0])
        
        mean_agg = float(sum(means) / len(means)) if means else 0.0
        std_agg = float(sum(stds) / len(stds)) if stds else 0.0

        # Binary and Hexadecimal formatting
        serialized = json.dumps(record).encode("utf-8")
        hex_frame = serialized.hex()
        binary_frame = "".join(format(b, '08b') for b in serialized[:48]) + ("..." if len(serialized) > 48 else "")

        # Commit to SQLite database
        self.cursor.execute(
            """
            INSERT INTO telemetry_archive 
            (timestamp, utc_time, node_signature, pipeline_mode, mean_aggregate, std_aggregate, binary_frame, hex_frame, raw_payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (timestamp, utc_time, node_sig, pipeline, mean_agg, std_agg, binary_frame, hex_frame, json.dumps(record))
        )
        self.conn.commit()

        # Professorial terminal output stream
        print(f"[PROCESSED] UTC: {utc_time} | Node: {node_sig[:12]}...")
        print(f"  STATS -> Mean Agg: {mean_agg:.6f} | Std Agg: {std_agg:.6f}")
        print(f"  HEX   -> {hex_frame[:64]}...")
        print(f"  BIN   -> {binary_frame}\n")

if __name__ == "__main__":
    processor = AcademicTelemetryProcessor()
    processor.process_stream()
                      
